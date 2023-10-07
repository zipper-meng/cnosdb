use std::collections::HashSet;
use std::time::Duration;

use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{get, middleware, post, web, App, HttpServer, Responder};
use config::Config;
use meta::error::MetaResult;
use meta::store::command::*;
use meta::store::storage::{response_encode, CommandResp, StateMachine};
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptionsBuilder, ROOT};
use models::oid::{Identifier, UuidGenerator};
use models::schema::{
    Tenant, TenantOptionsBuilder, DEFAULT_CATALOG, DEFAULT_DATABASE, USAGE_SCHEMA,
};
use openraft::error::ClientWriteError;
use openraft::raft::ClientWriteResponse;
use replication::apply_store::{ApplyStorage, HashMapSnapshotData};
use replication::{RaftNodeId, RaftNodeInfo, TypeConfig};
use trace::{debug, error};
use web::Json;

use crate::meta_single::Infallible;

pub struct MetaApp {
    pub http_addr: String,
    pub store: StateMachine,
}

#[post("/read")]
pub async fn read(app: Data<MetaApp>, req: Json<ReadCommand>) -> actix_web::Result<impl Responder> {
    let res = app.store.process_read_command(&req.0);

    let response: Result<CommandResp, Infallible> = Ok(res);
    Ok(Json(response))
}

#[post("/write")]
pub async fn write(
    app: Data<MetaApp>,
    req: Json<WriteCommand>,
) -> actix_web::Result<impl Responder> {
    let res = app.store.process_write_command(&req.0);

    let resp: Result<ClientWriteResponse<TypeConfig>, ClientWriteError<RaftNodeId, RaftNodeInfo>> =
        Ok(ClientWriteResponse::<TypeConfig> {
            log_id: Default::default(),
            data: res.into(),
            membership: None,
        });

    Ok(Json(resp))
}

#[get("/debug")]
pub async fn debug(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    if let Ok(data) = app.store.snapshot().await {
        let data: HashMapSnapshotData = serde_json::from_slice(&data)?;
        let mut rsp = "******---------------------------******\n".to_string();
        for (key, val) in data.map.iter() {
            rsp = rsp + &format!("* {}: {}\n", key, val);
        }
        rsp += "******----------------------------------------------******\n";

        Ok(rsp)
    } else {
        Ok("get snapshot data failed".to_string())
    }
}

#[post("/watch")]
pub async fn watch(
    app: Data<MetaApp>,
    req: Json<(String, String, HashSet<String>, u64)>, //client id, cluster,version
) -> actix_web::Result<impl Responder> {
    debug!("watch all  args: {:?}", req);
    let client = req.0 .0;
    let cluster = req.0 .1;
    let tenants = req.0 .2;
    let base_ver = req.0 .3;
    let mut follow_ver = base_ver;

    let mut notify = {
        let watch_data = app.store.read_change_logs(&cluster, &tenants, follow_ver);
        debug!(
            "{} {}.{}: change logs: {:?} ",
            client, base_ver, follow_ver, watch_data
        );
        if watch_data.need_return(base_ver) {
            let data = response_encode(Ok(watch_data));
            let response: Result<CommandResp, Infallible> = Ok(data);
            return Ok(Json(response));
        }

        app.store.watch.subscribe()
    };

    let now = std::time::Instant::now();
    loop {
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(20), notify.recv()).await;

        let watch_data = app.store.read_change_logs(&cluster, &tenants, follow_ver);
        debug!(
            "{} {}.{}: change logs: {:?} ",
            client, base_ver, follow_ver, watch_data
        );
        if watch_data.need_return(base_ver) || now.elapsed() > Duration::from_secs(30) {
            let data = response_encode(Ok(watch_data));
            let response: Result<CommandResp, Infallible> = Ok(data);
            return Ok(Json(response));
        }

        if follow_ver < watch_data.max_ver {
            follow_ver = watch_data.max_ver;
        }
    }
}

pub struct MetaService {
    cpu: usize,
    opt: Config,
}

impl MetaService {
    pub fn new(cpu: usize, opt: Config) -> Self {
        Self { cpu, opt }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        run_service(self.cpu, &self.opt).await
    }
}

pub async fn run_service(cpu: usize, opt: &Config) -> std::io::Result<()> {
    let db_path = format!("{}/meta/{}.data", opt.storage.path, 0);
    let state_machine = StateMachine::open(db_path).unwrap();

    let meta_service_addr = opt.cluster.meta_service_addr.clone();
    if meta_service_addr.len() > 1 {
        panic!("starting in singleton mode,only one meta is required");
    }

    let meta_service = opt.cluster.meta_service_addr.get(0).unwrap().clone();
    let app = Data::new(MetaApp {
        http_addr: meta_service.clone(),
        store: state_machine,
    });

    init_meta(&app, opt).await;
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            .service(write)
            .service(read)
            .service(debug)
            .service(watch)
    })
    .keep_alive(Duration::from_secs(5));

    let server = server.workers(cpu);

    let x = server.bind(meta_service)?;

    tokio::spawn(x.run());
    Ok(())
}

pub async fn init_meta(app: &Data<MetaApp>, opt: &Config) {
    // init user
    let user_opt_res = UserOptionsBuilder::default()
        .must_change_password(true)
        .comment("system admin")
        .build();
    let user_opt = match user_opt_res {
        Ok(opt) => opt,
        Err(_) => {
            error!("failed init admin user {}, exit init meta", ROOT);
            return;
        }
    };
    let oid = UuidGenerator::default().next_id();
    let user_desc = UserDesc::new(oid, ROOT.to_string(), user_opt, true);
    let req = WriteCommand::CreateUser(opt.cluster.name.clone(), user_desc);
    app.store.process_write_command(&req);

    // init tenant
    let tenant_opt = TenantOptionsBuilder::default()
        .comment("system tenant")
        .build()
        .expect("failed to init system tenant.");
    let oid = UuidGenerator::default().next_id();
    let tenant = Tenant::new(oid, DEFAULT_CATALOG.to_string(), tenant_opt);
    let req = WriteCommand::CreateTenant(opt.cluster.name.clone(), tenant);
    app.store.process_write_command(&req);

    // init role
    let req = ReadCommand::User(opt.cluster.name.clone(), ROOT.to_string());
    let user =
        serde_json::from_str::<MetaResult<Option<UserDesc>>>(&app.store.process_read_command(&req))
            .unwrap()
            .unwrap();
    if let Some(user_desc) = user {
        let role = TenantRoleIdentifier::System(SystemTenantRole::Owner);
        let req = WriteCommand::AddMemberToTenant(
            opt.cluster.name.clone(),
            *user_desc.id(),
            role,
            DEFAULT_CATALOG.to_string(),
        );
        app.store.process_write_command(&req);
    }

    // init database
    let req = WriteCommand::Set {
        key: format!(
            "/{}/tenants/{}/dbs/{}",
            opt.cluster.name, DEFAULT_CATALOG, DEFAULT_DATABASE
        ),
        value: format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null}}}}",
            DEFAULT_CATALOG, DEFAULT_DATABASE
        ),
    };
    app.store.process_write_command(&req);
    // init database
    let req = WriteCommand::Set {
        key: format!(
            "/{}/tenants/{}/dbs/{}",
            opt.cluster.name, DEFAULT_CATALOG, USAGE_SCHEMA
        ),
        value: format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null}}}}",
            DEFAULT_CATALOG, USAGE_SCHEMA
        ),
    };
    app.store.process_write_command(&req);
}
