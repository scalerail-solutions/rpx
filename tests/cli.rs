mod common;

use common::*;

#[test]
fn runs_rpx_help_inside_custom_r_image() {
    let container = start_container();
    let (exit_code, stdout, stderr) = run_command(&container, &["rpx", "--help"]);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Usage:"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn runs_rpx_run_with_isolated_library() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-run";
    create_package_project(&container, project_path);
    let command = format!(
        "mkdir -p {project_path} && cd {project_path} && rpx run Rscript -e \"cat(.libPaths()[1])\""
    );
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("rpx/libraries/"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}
