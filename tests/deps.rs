mod common;

use common::*;

fn write_description(
    container: &testcontainers::core::Container<testcontainers::GenericImage>,
    project_path: &str,
    contents: &str,
) {
    let command = format!(
        "mkdir -p {project_path} && cat > {project_path}/DESCRIPTION <<'EOF'\n{contents}\nEOF"
    );
    let (exit_code, stdout, stderr) = run_shell_command(container, &command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
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
        lockfile.contains("\"registry\""),
        "lockfile was: {lockfile}"
    );
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
        description.contains("Imports:\n    digest")
            || (description.contains("Imports:\n    digest (>=")
                && description.contains("digest (<<")),
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
fn runs_rpx_lock_without_installing_packages() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-lock";
    write_description(
        &container,
        project_path,
        "Package: testpkg
Version: 0.1.0
Title: Test Package
Description: Test package for rpx integration tests.
License: MIT
Author: Test Author
Maintainer: Test Author <test@example.com>
Imports: digest",
    );

    let command = format!("cd {project_path} && rpx lock");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert_package_state(&container, project_path, "digest", "FALSE");

    let lockfile = read_project_file(&container, project_path, "rpx.lock");
    assert!(
        lockfile.contains("\"registry\": \"https://api.rrepo.org\""),
        "lockfile was: {lockfile}"
    );
    assert!(
        lockfile.contains("https://api.rrepo.org/packages/digest/versions/"),
        "lockfile was: {lockfile}"
    );
}

#[test]
fn does_not_add_import_when_package_is_already_in_depends() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-add-depends";
    write_description(
        &container,
        project_path,
        "Package: testpkg
Version: 0.1.0
Title: Test Package
Description: Test package for rpx integration tests.
License: MIT
Author: Test Author
Maintainer: Test Author <test@example.com>
Depends: R (>= 4.3), digest",
    );

    let command = format!("cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        description.contains("Depends:\n    R (>= 4.3),\n    digest"),
        "DESCRIPTION was: {description}"
    );
    assert!(
        !description.contains("Imports: digest"),
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn removes_dependency_from_depends_while_preserving_r_requirement() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-remove-depends";
    write_description(
        &container,
        project_path,
        "Package: testpkg
Version: 0.1.0
Title: Test Package
Description: Test package for rpx integration tests.
License: MIT
Author: Test Author
Maintainer: Test Author <test@example.com>
Depends: R (>= 4.3), digest",
    );

    let install_command =
        format!("cd {project_path} && rpx run Rscript -e \"install.packages('digest')\"");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &install_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let command = format!("cd {project_path} && rpx remove digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &command);

    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        description.contains("Depends:\n    R (>= 4.3)"),
        "DESCRIPTION was: {description}"
    );
    assert!(
        !description.contains("digest"),
        "DESCRIPTION was: {description}"
    );
}
