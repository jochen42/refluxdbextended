use clap::CommandFactory;
use hashbrown::HashSet;

use crate::commands::serve::Config;

use super::*;

#[test]
fn test_sensitive_params_are_redacted() {
    let mut params = HashMap::new();
    for sensitive in SENSITIVE_PARAMS {
        params.insert(sensitive.to_string(), "un-redacted".to_string());
    }
    let result = capture_cli_params(params);
    let parsed = serde_json::from_str::<HashMap<String, String>>(&result).unwrap();
    assert_eq!(
        parsed.len(),
        SENSITIVE_PARAMS.len(),
        "expected there to be {n} parsed entries",
        n = SENSITIVE_PARAMS.len()
    );
    for sensitive in SENSITIVE_PARAMS {
        assert_eq!(
            parsed.get(*sensitive).unwrap(),
            REDACTED_STR,
            "expected {REDACTED_STR} for '{sensitive}' argument"
        );
    }
}

/// Extract all argument IDs from a Command recursively
fn extract_all_arg_ids(cmd: &clap::Command, args: &mut HashSet<String>) {
    for arg in cmd.get_arguments() {
        let id = arg.get_id().as_str();

        // Skip help and version which are always present
        if id == "help" || id == "version" || id == "help-all" {
            continue;
        }

        // Get the display name (long form or short form or id)
        let display_name = if let Some(long) = arg.get_long() {
            long.to_string()
        } else if let Some(short) = arg.get_short() {
            format!("{}", short)
        } else {
            id.to_string()
        };

        args.insert(display_name);
    }

    // Recursively process subcommands
    for subcmd in cmd.get_subcommands() {
        if subcmd.get_name() != "help" {
            extract_all_arg_ids(subcmd, args);
        }
    }
}

#[test]
fn test_all_config_params_categorized() {
    // Use the module-level constants - no need to redefine them here
    // Get all arguments from the Config command
    let cmd = Config::command();
    let mut discovered_args = HashSet::new();
    extract_all_arg_ids(
        cmd.get_subcommands()
            .find(|c| c.get_name() == "serve")
            .unwrap_or(&cmd),
        &mut discovered_args,
    );

    // If there are no serve subcommand, check the root
    if discovered_args.is_empty() {
        extract_all_arg_ids(&cmd, &mut discovered_args);
    }

    let mut uncategorized = Vec::new();

    for arg in &discovered_args {
        let is_in_non_sensitive_list = NON_SENSITIVE_PARAMS.contains(&arg.as_str());
        let is_in_sensitive_list = SENSITIVE_PARAMS.contains(&arg.as_str());

        if !is_in_non_sensitive_list && !is_in_sensitive_list {
            // Check if it might be caught by substring matching in is_sensitive function
            if !is_sensitive(arg) {
                uncategorized.push(arg.clone());
            }
        }
    }

    if !uncategorized.is_empty() {
        panic!(
            "The following CLI parameters are not categorized as either sensitive or \
            non-sensitive:\n{}\n\n\
            Please add them to either NON_SENSITIVE_PARAMS or SENSITIVE_PARAMS constants \
            at the module level.",
            uncategorized.join("\n")
        );
    }

    let mut needlessly_categorized = Vec::new();

    for arg in NON_SENSITIVE_PARAMS.iter().chain(SENSITIVE_PARAMS) {
        let is_discovered = discovered_args.contains(*arg);
        if !is_discovered {
            needlessly_categorized.push(arg.to_owned());
        }
    }

    if !needlessly_categorized.is_empty() {
        panic!(
            "The following CLI parameters were set as either sensitive or non-sensitive \
            but were not discovered in the actual command:\n{}\n\n\
            Please remove them from the NON_SENSITIVE_PARAMS or SENSITIVE_PARAMS constants.",
            needlessly_categorized.join("\n")
        );
    }
}

