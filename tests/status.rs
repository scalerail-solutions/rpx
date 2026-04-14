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

#[test]
fn runs_rpx_status_for_missing_library_package() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-status-missing-library";
    create_package_project(&container, project_path);

    let setup_command = format!("cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &setup_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let remove_package_dir = format!(
        "cd {project_path} && rm -rf \"$(rpx run Rscript -e \"cat(file.path(.libPaths()[1], 'digest'))\")\""
    );
    let (exit_code, stdout, stderr) = run_shell_command(&container, &remove_package_dir);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let status_command = format!("cd {project_path} && rpx status");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &status_command);
    assert_eq!(exit_code, 1, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Missing from library: digest"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn runs_rpx_status_for_extra_library_package() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-status-extra-library";
    create_package_project(&container, project_path);

    let setup_command = format!("cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &setup_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let extra_command =
        format!("cd {project_path} && rpx run Rscript -e \"install.packages('jsonlite')\"");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &extra_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let status_command = format!("cd {project_path} && rpx status");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &status_command);
    assert_eq!(exit_code, 1, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Extra in library: jsonlite"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn runs_rpx_status_for_version_mismatch() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-status-version-mismatch";
    create_package_project(&container, project_path);

    let setup_command = format!("cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &setup_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let mutate_lockfile = format!(
        "cd {project_path} && perl -0pi -e 's/\"version\": \"[0-9.]+\"/\"version\": \"0.0.1\"/' rpx.lock"
    );
    let (exit_code, stdout, stderr) = run_shell_command(&container, &mutate_lockfile);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let status_command = format!("cd {project_path} && rpx status");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &status_command);
    assert_eq!(exit_code, 1, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("Version mismatch: digest ("),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
    assert!(
        stdout.contains("0.0.1 locked"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}
