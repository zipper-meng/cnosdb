#![cfg(test)]

use reqwest::StatusCode;
use serial_test::serial;

use crate::case::step::{ControlStep, LineProtocol, RequestStep, Sql, StepPtr, StepResult};
use crate::case::E2eExecutor;
use crate::{cluster_def, E2eError};

//auto test about issue 669 799 842
#[test]
#[serial]
fn separated_start_test() {
    let executor = E2eExecutor::new_cluster(
        "computing_stroage_tests",
        "separated_start_test",
        cluster_def::one_meta_two_data_separated(),
    );
    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "write data to 8902 (invalid)",
            LineProtocol::build_request_with_str(
                "http://127.0.0.1:8902/api/v1/write?db=public",
                "start_test,ta=a fa=1 1",
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
        RequestStep::new_boxed(
            "write data to 8912",
            LineProtocol::build_request_with_str(
                "http://127.0.0.1:8912/api/v1/write?db=public",
                "start_test fa=1 1",
                Ok(()),
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "select data from 8902 (invalid)",
            Sql::build_request_with_str(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "select * from start_test",
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
        RequestStep::new_boxed(
            "select data from 8912",
            Sql::build_request_with_str(
                "http://127.0.0.1:8912/api/v1/sql?db=public",
                "select* from start_test",
                Ok(vec!["time,fa", "1970-01-01T00:00:00.000000001,1.0"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}

//auto test about issue 923
#[test]
#[serial]
fn meta_primary_crash_test() {
    let executor = E2eExecutor::new_cluster(
        "computing_stroage_tests",
        "meta_primary_crash_test",
        cluster_def::three_meta_two_data_bundled(),
    );

    let steps: Vec<StepPtr> = vec![
        ControlStep::new_boxed_sleep("sleep 10s", 10),
        ControlStep::new_boxed_stop_meta_node("stop meta node", 10),
        ControlStep::new_boxed_sleep("sleep 50s", 50),
        RequestStep::new_boxed(
            "write data",
            LineProtocol::build_request_with_str(
                "http://127.0.0.1:8912/api/v1/write?db=public",
                "start_test fa=1 1",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed(
            "select data",
            Sql::build_request_with_str(
                "http://127.0.0.1:8912/api/v1/sql?db=public",
                "select* from start_test",
                Ok(vec!["time,fa", "1970-01-01T00:00:00.000000001,1.0"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}
