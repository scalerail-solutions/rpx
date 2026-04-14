use std::path::PathBuf;

use testcontainers::{
    core::Container,
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

fn start_container() -> Container<testcontainers::GenericImage> {
    rpx_test_image()
        .with_cmd(vec!["sleep", "infinity"])
        .start()
        .expect("container should start")
}

fn run_shell_command(
    container: &Container<testcontainers::GenericImage>,
    command: &str,
) -> (i64, String, String) {
    run_command(container, &["sh", "-lc", command])
}

fn run_command(
    container: &Container<testcontainers::GenericImage>,
    command: &[&str],
) -> (i64, String, String) {
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

fn create_package_project(
    container: &testcontainers::core::Container<testcontainers::GenericImage>,
    project_path: &str,
) {
    let command = format!(
        "mkdir -p {project_path} && cat > {project_path}/DESCRIPTION <<'EOF'\nPackage: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for rpx integration tests.\nLicense: MIT\nAuthor: Test Author\nMaintainer: Test Author <test@example.com>\nEOF"
    );
    let (exit_code, stdout, stderr) = run_shell_command(container, &command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
}

fn assert_package_state(
    container: &testcontainers::core::Container<testcontainers::GenericImage>,
    project_path: &str,
    package: &str,
    expected: &str,
) {
    let check =
        format!("cat('{package}' %in% rownames(installed.packages(lib.loc = .libPaths()[1])))");
    let command =
        format!("mkdir -p {project_path} && cd {project_path} && rpx run Rscript -e \"{check}\"");
    let (exit_code, stdout, stderr) = run_shell_command(container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains(expected),
        "expected package state {expected}\nstdout was: {stdout}\nstderr was: {stderr}"
    );
}

fn read_project_file(
    container: &testcontainers::core::Container<testcontainers::GenericImage>,
    project_path: &str,
    file_name: &str,
) -> String {
    let command = format!("cd {project_path} && cat {file_name}");
    let (exit_code, stdout, stderr) = run_shell_command(container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    stdout
}

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
fn runs_rpx_add_inside_custom_r_image() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-add";
    let working_path = "/tmp/rpx-project-add/subdir";
    create_package_project(&container, project_path);

    let command = format!("mkdir -p {working_path} && cd {working_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert_package_state(&container, working_path, "digest", "TRUE");

    let lockfile = read_project_file(&container, project_path, "rpx.lock");
    assert!(lockfile.contains("\"digest\""), "lockfile was: {lockfile}");
    assert!(
        lockfile.contains("\"requirements\""),
        "lockfile was: {lockfile}"
    );
    assert!(
        lockfile.contains("\"packages\""),
        "lockfile was: {lockfile}"
    );

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        description.contains("Imports: digest"),
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn runs_rpx_remove_inside_custom_r_image() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-remove";
    create_package_project(&container, project_path);

    let add_command = format!("mkdir -p {project_path} && cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert_package_state(&container, project_path, "digest", "TRUE");

    let remove_command = format!("cd {project_path} && rpx remove digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &remove_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert_package_state(&container, project_path, "digest", "FALSE");

    let lockfile = read_project_file(&container, project_path, "rpx.lock");
    assert!(!lockfile.contains("\"digest\""), "lockfile was: {lockfile}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        !description.contains("digest"),
        "DESCRIPTION was: {description}"
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
fn runs_rpx_lock_from_current_library() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-lock";
    create_package_project(&container, project_path);
    let install_command = format!(
        "mkdir -p {project_path} && cd {project_path} && rpx run Rscript -e \"install.packages('digest')\""
    );
    let (exit_code, stdout, stderr) = run_shell_command(&container, &install_command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let lock_command = format!("cd {project_path} && rpx lock");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &lock_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let lockfile = read_project_file(&container, project_path, "rpx.lock");
    assert!(lockfile.contains("\"digest\""), "lockfile was: {lockfile}");
    assert!(
        lockfile.contains("\"requirements\": []"),
        "lockfile was: {lockfile}"
    );
}

#[test]
fn runs_rpx_sync_from_lockfile_without_mutating_it() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-sync";
    create_package_project(&container, project_path);
    let add_dependency =
        format!("cd {project_path} && cat >> DESCRIPTION <<'EOF'\nImports: digest\nEOF");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_dependency);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let seed_lockfile = format!(
        "mkdir -p {project_path} && cd {project_path} && cat > rpx.lock <<'EOF'\n{{\n  \"version\": 1,\n  \"requirements\": [\n    \"digest\"\n  ],\n  \"packages\": {{}}\n}}\nEOF"
    );
    let (exit_code, stdout, stderr) = run_shell_command(&container, &seed_lockfile);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let before = read_project_file(&container, project_path, "rpx.lock");

    let sync_command = format!("cd {project_path} && rpx sync");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &sync_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    assert_package_state(&container, project_path, "digest", "TRUE");

    let after = read_project_file(&container, project_path, "rpx.lock");
    assert_eq!(
        after, before,
        "lockfile changed during sync\nbefore:\n{before}\nafter:\n{after}"
    );
}

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
        "mkdir -p {project_path} && cd {project_path} && cat > rpx.lock <<'EOF'\n{{\n  \"version\": 1,\n  \"requirements\": [],\n  \"packages\": {{}}\n}}\nEOF"
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
