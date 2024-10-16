#![cfg(test)]

use reqwest::StatusCode;
use serial_test::serial;

use crate::case::step::{ControlStep, LineProtocol, RequestStep, Sql, StepPtr, StepResult};
use crate::case::E2eExecutor;
use crate::{cluster_def, E2eError};

#[test]
#[serial]
fn api_router() {
    let executor = E2eExecutor::new_cluster(
        "api_router_tests",
        "api_router",
        cluster_def::one_meta_two_data_separated(),
    );
    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "write data to 8902(invalid node)",
            LineProtocol::build_request_with_str(
                "http://127.0.0.1:8902/api/v1/write?db=public",
                "api_router,ta=a fa=1 1",
                Err(E2eError::Api {
                    status: StatusCode::NOT_FOUND,
                    url: None,
                    req: None,
                    resp: None,
                }),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_sleep("sleep 3s", 3),
        RequestStep::new_boxed(
            "write data to 8912",
            LineProtocol::build_request_with_str(
                "http://127.0.0.1:8912/api/v1/write?db=public",
                "api_router,ta=a fa=1 1",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_sleep("sleep 3s", 3),
        RequestStep::new_boxed(
            "query data from 8902 (invalid node)",
            Sql::build_request_with_str(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "select * from api_router",
                StepResult::Err(E2eError::Api {
                    status: StatusCode::NOT_FOUND,
                    url: None,
                    req: None,
                    resp: None,
                }),
                false,
                false,
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_sleep("sleep 3s", 3),
        RequestStep::new_boxed(
            "query data from 8912",
            Sql::build_request_with_str(
                "http://127.0.0.1:8912/api/v1/sql?db=public",
                "select * from api_router",
                Ok(vec!["time,ta,fa", "1970-01-01T00:00:00.000000001,a,1.0"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}
