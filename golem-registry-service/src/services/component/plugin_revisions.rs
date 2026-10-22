// Copyright 2024-2026 Golem Cloud
//
// Licensed under the Golem Source License v1.1 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://license.golem.cloud/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An installed oplog processor plugin stores the plugin component revision its
//! registration named when the component revision was written. The registration is the
//! authority: the built-in plugin provisioner moves a built-in plugin's registration to the
//! revision holding the bundled wasm, and every read of a component reports the revision
//! the registration names now - so source agents get the current plugin worker without
//! redeploying their component. User registrations never change their revision, so for
//! them this is a no-op.

use golem_common::model::component::{ComponentId, ComponentRevision, InstalledPlugin};
use golem_common::model::component_metadata::ComponentMetadata;
use golem_common::model::plugin_registration::PluginRegistrationId;
use std::collections::{BTreeSet, HashMap};

/// The oplog processor component (and its current revision) a registration names.
pub(super) type RegisteredOplogProcessors =
    HashMap<PluginRegistrationId, (ComponentId, ComponentRevision)>;

/// Registrations of the oplog processor plugins installed anywhere in `metadata`.
pub(super) fn installed_oplog_processor_registrations(
    metadata: &ComponentMetadata,
) -> BTreeSet<PluginRegistrationId> {
    installed_plugins(metadata)
        .filter(|plugin| plugin.oplog_processor_component_id.is_some())
        .map(|plugin| plugin.plugin_registration_id)
        .collect()
}

/// `metadata` with every installed oplog processor's revision set to the one its
/// registration names, or `None` when nothing differs. A registration naming another
/// component than the installation (never the case for a registration) is ignored.
pub(super) fn with_registered_revisions(
    metadata: &ComponentMetadata,
    registered: &RegisteredOplogProcessors,
) -> Option<ComponentMetadata> {
    let current_revision = |plugin: &InstalledPlugin| {
        let (component_id, revision) = registered.get(&plugin.plugin_registration_id)?;
        (plugin.oplog_processor_component_id == Some(*component_id)
            && plugin.oplog_processor_component_revision != Some(*revision))
        .then_some(*revision)
    };
    if !installed_plugins(metadata).any(|plugin| current_revision(plugin).is_some()) {
        return None;
    }
    let mut configs = metadata.agent_type_provision_configs().clone();
    for plugin in configs
        .values_mut()
        .flat_map(|config| config.plugins.iter_mut())
    {
        if let Some(revision) = current_revision(plugin) {
            plugin.oplog_processor_component_revision = Some(revision);
        }
    }
    Some(metadata.with_provision_configs(configs))
}

fn installed_plugins(metadata: &ComponentMetadata) -> impl Iterator<Item = &InstalledPlugin> {
    metadata
        .agent_type_provision_configs()
        .values()
        .flat_map(|config| config.plugins.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use golem_common::base_model::component_metadata::{AgentTypeProvisionConfig, KnownExports};
    use golem_common::model::agent::AgentTypeName;
    use golem_common::model::component::PluginPriority;
    use golem_common::model::environment_plugin_grant::EnvironmentPluginGrantId;
    use std::collections::BTreeMap;
    use test_r::test;

    fn revision(n: u64) -> ComponentRevision {
        ComponentRevision::new(n).unwrap()
    }

    fn installed(
        registration: PluginRegistrationId,
        component_id: ComponentId,
        rev: u64,
    ) -> InstalledPlugin {
        InstalledPlugin {
            environment_plugin_grant_id: EnvironmentPluginGrantId::new(),
            priority: PluginPriority(0),
            parameters: BTreeMap::new(),
            plugin_registration_id: registration,
            plugin_name: "golem-otlp-exporter".to_string(),
            plugin_version: "1.5.0".to_string(),
            oplog_processor_component_id: Some(component_id),
            oplog_processor_component_revision: Some(revision(rev)),
        }
    }

    fn metadata_with(plugins: Vec<InstalledPlugin>) -> ComponentMetadata {
        ComponentMetadata::from_parts(
            KnownExports::default(),
            vec![],
            None,
            None,
            vec![],
            BTreeMap::from([(
                AgentTypeName("Worker".to_string()),
                AgentTypeProvisionConfig {
                    plugins,
                    ..Default::default()
                },
            )]),
        )
    }

    fn revisions(metadata: &ComponentMetadata) -> Vec<Option<ComponentRevision>> {
        installed_plugins(metadata)
            .map(|plugin| plugin.oplog_processor_component_revision)
            .collect()
    }

    /// Regression test: a source component deployed while the built-in exporter was at
    /// revision 0 kept naming revision 0 after the registration moved to revision 1, so its
    /// agents kept running the outdated exporter until the component was redeployed.
    #[test]
    fn installed_revision_follows_the_registration() {
        let registration = PluginRegistrationId::new();
        let exporter = ComponentId::new();
        let metadata = metadata_with(vec![installed(registration, exporter, 0)]);
        let registered = HashMap::from([(registration, (exporter, revision(1)))]);

        let refreshed = with_registered_revisions(&metadata, &registered).unwrap();

        assert_eq!(revisions(&refreshed), vec![Some(revision(1))]);
    }

    #[test]
    fn unchanged_registration_changes_nothing() {
        let registration = PluginRegistrationId::new();
        let exporter = ComponentId::new();
        let metadata = metadata_with(vec![installed(registration, exporter, 2)]);
        let registered = HashMap::from([(registration, (exporter, revision(2)))]);

        assert!(with_registered_revisions(&metadata, &registered).is_none());
    }

    #[test]
    fn unknown_registration_or_other_component_changes_nothing() {
        let registration = PluginRegistrationId::new();
        let exporter = ComponentId::new();
        let metadata = metadata_with(vec![installed(registration, exporter, 0)]);
        let other_component = HashMap::from([(registration, (ComponentId::new(), revision(1)))]);

        assert!(with_registered_revisions(&metadata, &HashMap::new()).is_none());
        assert!(with_registered_revisions(&metadata, &other_component).is_none());
    }

    #[test]
    fn only_the_registered_plugins_are_refreshed() {
        let moved = PluginRegistrationId::new();
        let pinned = PluginRegistrationId::new();
        let exporter = ComponentId::new();
        let other = ComponentId::new();
        let metadata = metadata_with(vec![
            installed(moved, exporter, 0),
            installed(pinned, other, 4),
        ]);
        let registered = HashMap::from([
            (moved, (exporter, revision(3))),
            (pinned, (other, revision(4))),
        ]);

        let refreshed = with_registered_revisions(&metadata, &registered).unwrap();

        assert_eq!(
            revisions(&refreshed),
            vec![Some(revision(3)), Some(revision(4))]
        );
        assert_eq!(
            installed_oplog_processor_registrations(&metadata),
            BTreeSet::from([moved, pinned])
        );
    }
}
