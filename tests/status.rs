mod common;

use common::*;

#[test]
fn runs_rpx_status_for_clean_project() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-status-clean";
    create_package_project(&container, project_path);
    let setup_command = format!("mkdir -p {project_path} && cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &setup_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let status_command = format!("cd {project_path} && rpx status");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &status_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Status: ok"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn runs_rpx_status_for_lockfile_drift() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-status-drift";
    create_package_project(&container, project_path);
    let add_dependency =
        format!("cd {project_path} && cat >> DESCRIPTION <<'EOF'\nImports: digest\nEOF");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_dependency);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let seed_lockfile = format!(
        "mkdir -p {project_path} && cd {project_path} && cat > rpx.lock <<'EOF'\n{{\n  \"version\": 2,\n  \"requirements\": [],\n  \"repositories\": [\n    \"https://cloud.r-project.org\"\n  ],\n  \"packages\": {{}}\n}}\nEOF"
    );
    let (exit_code, stdout, stderr) = run_shell_command(&container, &seed_lockfile);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let status_command = format!("cd {project_path} && rpx status");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &status_command);
    assert_eq!(exit_code, 1, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Status: drift"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
    assert!(
        stdout.contains("Missing from lockfile: digest"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn runs_rpx_status_for_repository_drift() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-status-repo-drift";
    create_package_project(&container, project_path);

    let setup_command = format!("cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &setup_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let mutate_command = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &mutate_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let status_command = format!("cd {project_path} && rpx status");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &status_command);
    assert_eq!(exit_code, 1, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Repository mismatch:"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}
