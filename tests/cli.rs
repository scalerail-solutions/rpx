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

#[test]
fn runs_rpx_init_in_empty_directory() {
    let container = start_container();
    let project_path = "/tmp/new-rpx-project";
    let command = format!("mkdir -p {project_path} && cd {project_path} && rpx init");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Initialized project at"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );

    let description =
        run_shell_command(&container, &format!("cd {project_path} && cat DESCRIPTION"));
    assert_eq!(
        description.0, 0,
        "stdout was: {}\nstderr was: {}",
        description.1, description.2
    );
    assert!(
        description.1.contains("Package: new.rpx.project"),
        "DESCRIPTION was: {}",
        description.1
    );
    assert!(
        description.1.contains("Title: New Rpx Project"),
        "DESCRIPTION was: {}",
        description.1
    );
}

#[test]
fn fails_when_description_already_exists() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-init-existing";
    create_package_project(&container, project_path);

    let command = format!("cd {project_path} && rpx init");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 101, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stderr.contains("DESCRIPTION already exists"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn init_creates_project_that_can_add_dependencies() {
    let container = start_container();
    let project_path = "/tmp/rpx-init-add";
    let command =
        format!("mkdir -p {project_path} && cd {project_path} && rpx init && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let lockfile = run_shell_command(&container, &format!("cd {project_path} && cat rpx.lock"));
    assert_eq!(
        lockfile.0, 0,
        "stdout was: {}\nstderr was: {}",
        lockfile.1, lockfile.2
    );
    assert!(
        lockfile.1.contains("\"digest\""),
        "lockfile was: {}",
        lockfile.1
    );
}
