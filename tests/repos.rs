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

#[test]
fn runs_rpx_repo_add_and_list() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-add";
    create_package_project(&container, project_path);

    let add_command = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        description
            .contains("Additional_repositories: https://packagemanager.posit.co/cran/latest"),
        "DESCRIPTION was: {description}"
    );

    let list_command = format!("cd {project_path} && rpx repo list");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &list_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.contains("CRAN: https://cloud.r-project.org"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
    assert!(
        stdout.contains("posit: https://packagemanager.posit.co/cran/latest"),
        "stdout was: {stdout}\nstderr was: {stderr}"
    );
}

#[test]
fn does_not_duplicate_repo_when_added_twice() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-dedupe";
    create_package_project(&container, project_path);

    let first_add = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &first_add);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let second_add = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &second_add);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert_eq!(
        description
            .matches("https://packagemanager.posit.co/cran/latest")
            .count(),
        1,
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn runs_rpx_repo_add_and_remove_bioconductor() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-bioconductor";
    create_package_project(&container, project_path);

    let add_command = format!("cd {project_path} && rpx repo add bioconductor");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        description.contains("https://bioconductor.org/packages/release/bioc"),
        "DESCRIPTION was: {description}"
    );
    assert!(
        description.contains("https://bioconductor.org/packages/release/data/annotation"),
        "DESCRIPTION was: {description}"
    );
    assert!(
        description.contains("https://bioconductor.org/packages/release/data/experiment"),
        "DESCRIPTION was: {description}"
    );

    let list_command = format!("cd {project_path} && rpx repo list");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &list_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
    assert!(
        stdout.matches("bioconductor:").count() == 3,
        "stdout was: {stdout}\nstderr was: {stderr}"
    );

    let remove_command = format!("cd {project_path} && rpx repo remove bioconductor");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &remove_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        !description.contains("Additional_repositories"),
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn runs_rpx_repo_remove_raw_url_added_by_alias() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-remove-url";
    create_package_project(&container, project_path);

    let add_command = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let remove_command =
        format!("cd {project_path} && rpx repo remove https://packagemanager.posit.co/cran/latest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &remove_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        !description.contains("Additional_repositories"),
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn runs_rpx_repo_remove_alias_added_by_raw_url() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-remove-alias";
    create_package_project(&container, project_path);

    let add_command = format!("cd {project_path} && rpx repo add https://R-Forge.R-project.org");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let remove_command = format!("cd {project_path} && rpx repo remove r-forge");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &remove_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        !description.contains("Additional_repositories"),
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn preserves_existing_additional_repositories_when_adding_repo() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-roundtrip";
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
Additional_repositories: https://example.com/repo,
 https://example.org/repo",
    );

    let add_command = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let description = read_project_file(&container, project_path, "DESCRIPTION");
    assert!(
        description.contains("https://example.com/repo"),
        "DESCRIPTION was: {description}"
    );
    assert!(
        description.contains("https://example.org/repo"),
        "DESCRIPTION was: {description}"
    );
    assert!(
        description.contains("https://packagemanager.posit.co/cran/latest"),
        "DESCRIPTION was: {description}"
    );
}

#[test]
fn relocks_automatically_when_adding_repo_with_existing_lockfile() {
    let container = start_container();
    let project_path = "/tmp/rpx-project-repo-relock";
    create_package_project(&container, project_path);

    let setup_command = format!("cd {project_path} && rpx add digest");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &setup_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let add_command = format!("cd {project_path} && rpx repo add posit");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &add_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");

    let lockfile = read_project_file(&container, project_path, "rpx.lock");
    assert!(
        lockfile.contains("\"registry\": \"https://packagemanager.posit.co/cran/latest\""),
        "lockfile was: {lockfile}"
    );

    let sync_command = format!("cd {project_path} && rpx sync");
    let (exit_code, stdout, stderr) = run_shell_command(&container, &sync_command);
    assert_eq!(exit_code, 0, "stdout was: {stdout}\nstderr was: {stderr}");
}
