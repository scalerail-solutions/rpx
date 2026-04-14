use std::path::PathBuf;

use testcontainers::{
    core::ExecCommand,
    runners::{SyncBuilder, SyncRunner},
    GenericBuildableImage, ImageExt,
};

fn rpx_test_image() -> testcontainers::GenericImage {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    GenericBuildableImage::new("rpx-test", "latest")
        .with_dockerfile(root.join("tests/docker/rpx.Dockerfile"))
        .with_file(root.join("Cargo.toml"), "./Cargo.toml")
        .with_file(root.join("Cargo.lock"), "./Cargo.lock")
        .with_file(root.join("src"), "./src")
        .build_image()
        .expect("image should build")
}

fn run_command(command: &[&str]) -> (i64, String, String) {
    let container = rpx_test_image()
        .with_cmd(vec!["sleep", "infinity"])
        .start()
        .expect("container should start");

    let mut result = container
        .exec(ExecCommand::new(command.iter().copied()))
        .expect("command should run");

    let stdout = String::from_utf8(result.stdout_to_vec().expect("should read stdout"))
        .expect("stdout should be utf-8");
    let stderr = String::from_utf8(result.stderr_to_vec().expect("should read stderr"))
        .expect("stderr should be utf-8");

    let exit_code = result
        .exit_code()
        .expect("should read exit code")
        .expect("command should have exited");

    (exit_code, stdout, stderr)
}

fn assert_command_runs(command: &[&str]) {
    let (exit_code, stdout, stderr) = run_command(command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
}

#[test]
fn runs_rpx_help_inside_custom_r_image() {
    let (exit_code, stdout, stderr) = run_command(&["rpx", "--help"]);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Usage:"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn runs_rpx_add_inside_custom_r_image() {
    assert_command_runs(&["rpx", "add"]);
}

#[test]
fn runs_rpx_remove_inside_custom_r_image() {
    assert_command_runs(&["rpx", "remove"]);
}
