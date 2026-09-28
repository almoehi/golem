// Copyright 2024-2026 Golem Cloud
//
// Licensed under the Golem Source Available License v1.1 (the "License");
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

use super::ComponentError;
use crate::metrics::storage::record_component_uploaded;
use crate::repo::component::ComponentRepo;
use crate::repo::model::component::{ComponentRepoError, ComponentRevisionRecord};
use crate::services::account_usage::AccountUsageService;
use crate::services::component::utils::prepare_component_files_for_upload;
use crate::services::component_compilation::ComponentCompilationService;
use crate::services::component_object_store::ComponentObjectStore;
use crate::services::environment::EnvironmentError;
use crate::services::environment::EnvironmentService;
use crate::services::environment_plugin_grant::{
    EnvironmentPluginGrantError, EnvironmentPluginGrantService,
};
use crate::services::run_cpu_bound_work;
use anyhow::Context;
use golem_common::base_model::component_metadata::AgentTypeProvisionConfig;
use golem_common::base_model::environment_plugin_grant::EnvironmentPluginGrantWithDetails;
use golem_common::model::agent::{AgentConfigSource, AgentType};
use golem_common::model::agent::{AgentFileContentHash, AgentTypeName};
use golem_common::model::component::{
    AgentFilePath, ArchiveFilePath, ComponentCreation, ComponentId, ComponentRevision,
    ComponentUpdate, InitialAgentFile, InstalledPlugin, PluginInstallation,
    PluginInstallationAction, PluginPriority,
};
use golem_common::model::component::{
    AgentTypeProvisionConfigCreation, AgentTypeProvisionConfigUpdate,
};
use golem_common::model::component_metadata::ComponentMetadata;
use golem_common::model::diff::Hash;
use golem_common::model::environment::{Environment, EnvironmentId};
use golem_common::model::environment_plugin_grant::EnvironmentPluginGrantId;
use golem_common::model::worker::AgentConfigEntryDto;
use golem_common::model::worker::TypedAgentConfigEntry;
use golem_service_base::model::auth::AuthCtx;
use golem_service_base::model::auth::EnvironmentAction;
use golem_service_base::model::component::Component;
use golem_service_base::replayable_stream::ReplayableStream;
use golem_service_base::service::initial_agent_files::InitialAgentFilesService;
use golem_wasm::ValueAndType;
use golem_wasm::json::ValueAndTypeJsonExtensions;
use itertools::Itertools;
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tracing::info;

pub struct ComponentWriteService {
    component_repo: Arc<dyn ComponentRepo>,
    object_store: Arc<ComponentObjectStore>,
    component_compilation: Arc<dyn ComponentCompilationService>,
    initial_agent_files_service: Arc<InitialAgentFilesService>,
    account_usage_service: Arc<AccountUsageService>,
    environment_service: Arc<EnvironmentService>,
    environment_plugin_grant_service: Arc<EnvironmentPluginGrantService>,
}

impl ComponentWriteService {
    pub fn new(
        component_repo: Arc<dyn ComponentRepo>,
        object_store: Arc<ComponentObjectStore>,
        component_compilation: Arc<dyn ComponentCompilationService>,
        initial_agent_files_service: Arc<InitialAgentFilesService>,
        account_usage_service: Arc<AccountUsageService>,
        environment_service: Arc<EnvironmentService>,
        environment_plugin_grant_service: Arc<EnvironmentPluginGrantService>,
    ) -> Self {
        Self {
            component_repo,
            object_store,
            component_compilation,
            initial_agent_files_service,
            account_usage_service,
            environment_service,
            environment_plugin_grant_service,
        }
    }

    pub async fn create(
        &self,
        environment_id: EnvironmentId,
        component_creation: ComponentCreation,
        wasm: Vec<u8>,
        files_archive: Option<NamedTempFile>,
        auth: &AuthCtx,
    ) -> Result<Component, ComponentError> {
        info!(environment_id = %environment_id, "Create component");

        let wasm: Arc<[u8]> = Arc::from(wasm);

        let environment = self
            .environment_service
            .get(environment_id, false, auth)
            .await
            .map_err(|err| match err {
                EnvironmentError::EnvironmentNotFound(environment_id) => {
                    ComponentError::ParentEnvironmentNotFound(environment_id)
                }
                other => other.into(),
            })?;

        auth.authorize_environment_action(
            environment.owner_account_id,
            &environment.roles_from_active_shares,
            EnvironmentAction::CreateComponent,
        )?;

        // Fast path check to avoid processing the component if we are going to reject it anyway
        self.component_repo
            .get_staged_by_name(environment_id.0, &component_creation.component_name.0)
            .await?
            .map_or(Ok(()), |_| {
                Err(ComponentError::ComponentWithNameAlreadyExists(
                    component_creation.component_name.clone(),
                ))
            })?;

        self.account_usage_service
            .ensure_new_component_within_limits(
                environment.owner_account_id,
                u64::try_from(wasm.len()).unwrap(),
            )
            .await?;

        let referenced_paths: HashSet<ArchiveFilePath> = component_creation
            .agent_type_provision_configs
            .values()
            .flat_map(|c| c.files.keys().cloned())
            .collect();
        let uploaded_files = match files_archive {
            Some(archive) => {
                self.upload_agent_files(environment_id, archive, &referenced_paths)
                    .await?
            }
            None => HashMap::new(),
        };

        let component_id = ComponentId::new();
        let wasm_bytes = wasm.len() as u64;
        let (wasm_hash, wasm_object_store_key) = self
            .upload_and_hash_component_wasm(environment_id, wasm.clone())
            .await?;
        record_component_uploaded(
            &auth.actor_account_id().to_string(),
            &environment_id.to_string(),
            wasm_bytes,
        );

        // Batch-resolve all plugin grants referenced across all agent types in one pass,
        // so the same grant is only fetched once even if shared by multiple agent types.
        let all_grant_ids: HashSet<EnvironmentPluginGrantId> = component_creation
            .agent_type_provision_configs
            .values()
            .flat_map(|c| {
                c.plugin_installations
                    .iter()
                    .map(|p| p.environment_plugin_grant_id)
            })
            .collect();
        let resolved_grants = self
            .resolve_all_plugin_grants(&environment, all_grant_ids, auth)
            .await?;

        let mut provision_configs: BTreeMap<AgentTypeName, AgentTypeProvisionConfig> =
            BTreeMap::new();

        for (agent_type_name, creation) in &component_creation.agent_type_provision_configs {
            let agent_type = component_creation
                .agent_types
                .iter()
                .find(|t| &t.type_name == agent_type_name)
                .ok_or_else(|| {
                    ComponentError::UndeclaredAgentTypeInProvisionConfig(agent_type_name.clone())
                })?;

            let files = resolve_files_for_creation(agent_type_name, creation, &uploaded_files)?;
            let plugins =
                resolve_plugins_for_creation(&creation.plugin_installations, &resolved_grants)?;
            let config =
                validate_and_transform_config_entries(agent_type, creation.config.clone())?;

            provision_configs.insert(
                agent_type_name.clone(),
                AgentTypeProvisionConfig {
                    env: creation.env.clone(),
                    config,
                    plugins,
                    files,
                },
            );
        }

        let component_metadata = analyze_and_validate_component_wasm(
            component_creation.agent_types,
            wasm.clone(),
            provision_configs,
        )
        .await?;
        validate_component_metadata_invariants(&component_metadata)?;

        let component_size = wasm.len() as u64;

        let record = ComponentRevisionRecord::creation(
            component_id,
            component_size,
            component_metadata,
            wasm_hash,
            wasm_object_store_key,
            auth.actor_account_id(),
        );

        let stored_component: Component = self
            .component_repo
            .create(
                environment_id.0,
                &component_creation.component_name.0,
                record,
            )
            .await
            .map_err(|err| match err {
                ComponentRepoError::ConcurrentModification
                | ComponentRepoError::VersionAlreadyExists { .. } => {
                    ComponentError::ConcurrentUpdate
                }
                ComponentRepoError::ComponentViolatesUniqueness => {
                    ComponentError::ComponentWithNameAlreadyExists(
                        component_creation.component_name,
                    )
                }
                other => other.into(),
            })?
            .try_into_model(environment.application_id, environment.owner_account_id)?;

        self.component_compilation
            .enqueue_compilation(environment_id, component_id, stored_component.revision)
            .await;

        Ok(stored_component)
    }

    pub async fn update(
        &self,
        component_id: ComponentId,
        component_update: ComponentUpdate,
        new_wasm: Option<Vec<u8>>,
        new_files_archive: Option<NamedTempFile>,
        auth: &AuthCtx,
    ) -> Result<Component, ComponentError> {
        let new_wasm: Option<Arc<[u8]>> = new_wasm.map(Arc::from);

        let component_record = self
            .component_repo
            .get_staged_by_id(component_id.0)
            .await?
            .ok_or(ComponentError::ComponentNotFound(component_id))?;

        let environment = self
            .environment_service
            .get(EnvironmentId(component_record.environment_id), false, auth)
            .await
            .map_err(|err| match err {
                EnvironmentError::EnvironmentNotFound(_) => {
                    ComponentError::ComponentNotFound(component_id)
                }
                other => other.into(),
            })?;

        auth.authorize_environment_action(
            environment.owner_account_id,
            &environment.roles_from_active_shares,
            EnvironmentAction::ViewComponent,
        )
        .map_err(|_| ComponentError::ComponentNotFound(component_id))?;

        auth.authorize_environment_action(
            environment.owner_account_id,
            &environment.roles_from_active_shares,
            EnvironmentAction::UpdateComponent,
        )?;

        if component_update.allow_incompatible_config && environment.compatibility_check {
            return Err(ComponentError::ResetOverrideRequiresCompatibilityCheckDisabled);
        }

        let mut component = component_record
            .try_into_model(environment.application_id, environment.owner_account_id)?;

        if component_update.current_revision != component.revision {
            Err(ComponentError::ConcurrentUpdate)?
        };

        component.revision = component.revision.next()?;

        let environment_id = component.environment_id;
        let component_id = component.id;

        info!(environment_id = %environment_id, "Update component");

        let agent_types_changed = component_update.agent_types.is_some();
        let allow_incompatible_config = component_update.allow_incompatible_config;

        let agent_types = component_update
            .agent_types
            .unwrap_or(component.metadata.agent_types().to_vec());

        let mut final_provision_configs = component.metadata.agent_type_provision_configs().clone();
        if agent_types_changed {
            final_provision_configs =
                provision_configs_for_agent_types(&agent_types, final_provision_configs);
        }

        let mut provision_configs_changed = false;

        if let Some(updates) = component_update.agent_type_provision_config_updates {
            provision_configs_changed = true;

            let referenced_paths: HashSet<ArchiveFilePath> = updates
                .values()
                .flat_map(|u| u.files_to_add_or_update.keys().cloned())
                .collect();
            let uploaded_files = match new_files_archive {
                Some(archive) => {
                    self.upload_agent_files(environment_id, archive, &referenced_paths)
                        .await?
                }
                None => HashMap::new(),
            };

            for (agent_type_name, update) in updates {
                let agent_type = agent_types
                    .iter()
                    .find(|agent_type| agent_type.type_name == agent_type_name)
                    .ok_or_else(|| {
                        ComponentError::UndeclaredAgentTypeInProvisionConfig(
                            agent_type_name.clone(),
                        )
                    })?;

                let existing = final_provision_configs
                    .get(&agent_type_name)
                    .cloned()
                    .unwrap_or_default();

                let updated = self
                    .apply_provision_config_update(
                        &agent_type_name,
                        existing,
                        update,
                        &uploaded_files,
                        agent_type,
                        &environment,
                        auth,
                    )
                    .await?;

                final_provision_configs.insert(agent_type_name, updated);
            }
        }

        if agent_types_changed && !allow_incompatible_config {
            for (agent_type_name, config) in &final_provision_configs {
                let agent_type = agent_types
                    .iter()
                    .find(|agent_type| &agent_type.type_name == agent_type_name)
                    .ok_or_else(|| {
                        ComponentError::UndeclaredAgentTypeInProvisionConfig(
                            agent_type_name.clone(),
                        )
                    })?;
                check_config_entries_match(agent_type, &config.config)?;
            }
        }

        if let Some(new_wasm) = new_wasm {
            self.account_usage_service
                .ensure_updated_component_within_limits(
                    environment.owner_account_id,
                    u64::try_from(new_wasm.len()).unwrap(),
                    component.component_size,
                )
                .await?;

            let new_wasm_bytes = new_wasm.len() as u64;
            let (wasm_hash, wasm_object_store_key) = self
                .upload_and_hash_component_wasm(environment_id, new_wasm.clone())
                .await?;
            record_component_uploaded(
                &auth.actor_account_id().to_string(),
                &environment_id.to_string(),
                new_wasm_bytes,
            );

            component.wasm_hash = wasm_hash;
            component.object_store_key = wasm_object_store_key;
            component.metadata = analyze_and_validate_component_wasm(
                agent_types,
                new_wasm.clone(),
                final_provision_configs,
            )
            .await?;
        } else if agent_types_changed {
            // TODO: skip the download here
            let old_data = self
                .object_store
                .get(environment_id, &component.object_store_key)
                .await?;

            component.metadata = analyze_and_validate_component_wasm(
                agent_types,
                Arc::from(old_data),
                final_provision_configs,
            )
            .await?;
        } else if provision_configs_changed {
            component.metadata = component
                .metadata
                .with_provision_configs(final_provision_configs);
        }

        validate_component_metadata_invariants(&component.metadata)?;

        let record = ComponentRevisionRecord::from_model(component, auth.actor_account_id());

        let stored_component: Component = self
            .component_repo
            .update(record)
            .await
            .map_err(|err| match err {
                ComponentRepoError::ConcurrentModification => ComponentError::ConcurrentUpdate,
                ComponentRepoError::VersionAlreadyExists { version } => {
                    ComponentError::ComponentVersionAlreadyExists(version)
                }
                other => other.into(),
            })?
            .try_into_model(environment.application_id, environment.owner_account_id)?;

        self.component_compilation
            .enqueue_compilation(environment_id, component_id, stored_component.revision)
            .await;

        Ok(stored_component)
    }

    pub async fn delete(
        &self,
        component_id: ComponentId,
        current_revision: ComponentRevision,
        auth: &AuthCtx,
    ) -> Result<(), ComponentError> {
        let component_record = self
            .component_repo
            .get_staged_by_id(component_id.0)
            .await?
            .ok_or(ComponentError::ComponentNotFound(component_id))?;

        let environment = self
            .environment_service
            .get(EnvironmentId(component_record.environment_id), false, auth)
            .await
            .map_err(|err| match err {
                EnvironmentError::EnvironmentNotFound(_) => {
                    ComponentError::ComponentNotFound(component_id)
                }
                other => other.into(),
            })?;

        auth.authorize_environment_action(
            environment.owner_account_id,
            &environment.roles_from_active_shares,
            EnvironmentAction::ViewComponent,
        )
        .map_err(|_| ComponentError::ComponentNotFound(component_id))?;

        auth.authorize_environment_action(
            environment.owner_account_id,
            &environment.roles_from_active_shares,
            EnvironmentAction::UpdateComponent,
        )?;

        let component = component_record
            .try_into_model(environment.application_id, environment.owner_account_id)?;

        if current_revision != component.revision {
            Err(ComponentError::ConcurrentUpdate)?
        };

        self.component_repo
            .delete(
                auth.actor_account_id().0,
                component_id.0,
                current_revision.next()?.into(),
            )
            .await
            .map_err(|err| match err {
                ComponentRepoError::ConcurrentModification => ComponentError::ConcurrentUpdate,
                other => other.into(),
            })?;

        Ok(())
    }

    async fn upload_and_hash_component_wasm(
        &self,
        environment_id: EnvironmentId,
        data: Arc<[u8]>,
    ) -> Result<(Hash, String), ComponentError> {
        // TODO: use something like PluginWasmFilesService instead of raw object store
        let hash = self.object_store.put(environment_id, data).await?;
        Ok((hash, hash.to_string()))
    }

    async fn upload_agent_files(
        &self,
        environment_id: EnvironmentId,
        archive: NamedTempFile,
        referenced_paths: &HashSet<ArchiveFilePath>,
    ) -> Result<HashMap<ArchiveFilePath, (AgentFileContentHash, u64)>, ComponentError> {
        let to_upload = prepare_component_files_for_upload(archive)
            .await?
            .into_iter()
            .filter(|(path, _)| referenced_paths.contains(path))
            .collect::<Vec<_>>();

        let tasks = to_upload.into_iter().map(|(path, stream)| async move {
            info!("Uploading file: {}", path.to_string());

            let size = stream
                .length()
                .await
                .context("Failed to get component file size")?;

            let key = self
                .initial_agent_files_service
                .put_if_not_exists(environment_id, &stream)
                .await
                .context("Failed to upload component files")?;

            Ok::<_, ComponentError>((path, (key, size)))
        });

        let uploaded = futures::future::try_join_all(tasks).await?;

        Ok(HashMap::from_iter(uploaded))
    }

    /// Resolves all plugin grants in a single DB query.
    /// Deduplicates by grant ID so the same grant is fetched at most once,
    /// even if referenced by multiple agent types.
    async fn resolve_all_plugin_grants(
        &self,
        environment: &Environment,
        grant_ids: impl IntoIterator<Item = EnvironmentPluginGrantId>,
        auth: &AuthCtx,
    ) -> Result<HashMap<EnvironmentPluginGrantId, EnvironmentPluginGrantWithDetails>, ComponentError>
    {
        self.environment_plugin_grant_service
            .get_active_by_ids_for_environment(grant_ids, environment, auth)
            .await
            .map_err(|err| match err {
                EnvironmentPluginGrantError::EnvironmentPluginGrantNotFound(id) => {
                    ComponentError::EnvironmentPluginNotFound(id)
                }
                other => other.into(),
            })
    }

    /// Resolves the grants referenced by `Install` actions in one query, then applies the
    /// actions to the previous installations (see [`apply_plugin_installation_actions`]).
    async fn update_plugin_installations(
        &self,
        environment: &Environment,
        previous: Vec<InstalledPlugin>,
        updates: Vec<PluginInstallationAction>,
        auth: &AuthCtx,
    ) -> Result<Vec<InstalledPlugin>, ComponentError> {
        let install_grant_ids: HashSet<EnvironmentPluginGrantId> = updates
            .iter()
            .filter_map(|update| match update {
                PluginInstallationAction::Install(inner) => Some(inner.environment_plugin_grant_id),
                _ => None,
            })
            .collect();

        let resolved_grants = if install_grant_ids.is_empty() {
            HashMap::new()
        } else {
            self.resolve_all_plugin_grants(environment, install_grant_ids, auth)
                .await?
        };

        apply_plugin_installation_actions(previous, updates, &resolved_grants)
    }

    async fn apply_provision_config_update(
        &self,
        agent_type_name: &AgentTypeName,
        existing: AgentTypeProvisionConfig,
        update: AgentTypeProvisionConfigUpdate,
        uploaded_files: &HashMap<ArchiveFilePath, (AgentFileContentHash, u64)>,
        agent_type: &AgentType,
        environment: &Environment,
        auth: &AuthCtx,
    ) -> Result<AgentTypeProvisionConfig, ComponentError> {
        // Env
        let env = update.env.unwrap_or(existing.env);

        // Config entries: validate and transform new ones, or keep existing
        let config = if let Some(new_config) = update.config {
            validate_and_transform_config_entries(agent_type, new_config)?
        } else {
            existing.config
        };

        // Files: start from existing, remove removed, add/update new ones
        let removed: HashSet<AgentFilePath> = HashSet::from_iter(update.files_to_remove);
        let mut files: HashMap<AgentFilePath, InitialAgentFile> = existing
            .files
            .into_iter()
            .filter(|f| !removed.contains(&f.path))
            .map(|f| (f.path.clone(), f))
            .collect();

        for (archive_path, options) in &update.files_to_add_or_update {
            let (content_hash, size) = uploaded_files.get(archive_path).ok_or_else(|| {
                ComponentError::AgentFileNotFoundInArchive {
                    agent_type: agent_type_name.clone(),
                    archive_path: archive_path.clone(),
                }
            })?;
            files.insert(
                options.target_path.clone(),
                InitialAgentFile {
                    path: options.target_path.clone(),
                    content_hash: *content_hash,
                    permissions: options.permissions,
                    size: *size,
                },
            );
        }

        for (target_path, permissions) in &update.file_permission_updates {
            if let Some(file) = files.get_mut(target_path) {
                file.permissions = *permissions;
            }
        }

        let files = files.into_values().collect();

        // Plugins
        let plugins = self
            .update_plugin_installations(environment, existing.plugins, update.plugin_updates, auth)
            .await?;

        Ok(AgentTypeProvisionConfig {
            env,
            config,
            plugins,
            files,
        })
    }
}

fn resolve_files_for_creation(
    agent_type_name: &AgentTypeName,
    creation: &AgentTypeProvisionConfigCreation,
    uploaded_files: &HashMap<ArchiveFilePath, (AgentFileContentHash, u64)>,
) -> Result<Vec<InitialAgentFile>, ComponentError> {
    creation
        .files
        .iter()
        .map(|(archive_path, options)| {
            let (content_hash, size) = uploaded_files.get(archive_path).ok_or_else(|| {
                ComponentError::AgentFileNotFoundInArchive {
                    agent_type: agent_type_name.clone(),
                    archive_path: archive_path.clone(),
                }
            })?;
            Ok(InitialAgentFile {
                path: options.target_path.clone(),
                content_hash: *content_hash,
                permissions: options.permissions,
                size: *size,
            })
        })
        .collect()
}

fn resolve_plugins_for_creation(
    plugin_installations: &[PluginInstallation],
    resolved_grants: &HashMap<EnvironmentPluginGrantId, EnvironmentPluginGrantWithDetails>,
) -> Result<Vec<InstalledPlugin>, ComponentError> {
    let mut result: Vec<InstalledPlugin> = Vec::new();

    for plugin_installation in plugin_installations {
        // ensure the plugin priority is not already used within this agent type
        if result
            .iter()
            .any(|p| p.priority == plugin_installation.priority)
        {
            return Err(ComponentError::ConflictingPluginPriority(
                plugin_installation.priority,
            ));
        };

        if result.iter().any(|p| {
            p.environment_plugin_grant_id == plugin_installation.environment_plugin_grant_id
        }) {
            return Err(ComponentError::ConflictingEnvironmentPluginGrantId(
                plugin_installation.environment_plugin_grant_id,
            ));
        };

        // look up the pre-resolved grant details
        let grant = resolved_grants
            .get(&plugin_installation.environment_plugin_grant_id)
            .ok_or(ComponentError::EnvironmentPluginNotFound(
                plugin_installation.environment_plugin_grant_id,
            ))?;

        result.push(installed_plugin_from_grant(
            grant,
            plugin_installation.priority,
            plugin_installation.parameters.clone(),
        ));
    }

    Ok(result)
}

/// Builds the stored installation record for `grant` with the given priority and parameters.
fn installed_plugin_from_grant(
    grant: &EnvironmentPluginGrantWithDetails,
    priority: PluginPriority,
    parameters: BTreeMap<String, String>,
) -> InstalledPlugin {
    InstalledPlugin {
        environment_plugin_grant_id: grant.id,
        parameters,
        priority,
        plugin_registration_id: grant.plugin.id,
        oplog_processor_component_id: grant.plugin.oplog_processor_component_id(),
        oplog_processor_component_revision: grant.plugin.oplog_processor_component_revision(),
        plugin_name: grant.plugin.name.clone(),
        plugin_version: grant.plugin.version.clone(),
    }
}

/// Applies plugin installation actions to a component revision's previous installations.
///
/// Installations are keyed by environment plugin grant id. Priority uniqueness is an invariant
/// of the resulting set, so it is checked once on the final state rather than per action —
/// checking per action compared an installation against itself (a re-install or a no-op
/// priority update collided with its own priority, #217) and rejected valid batches such as
/// priority swaps.
///
/// `Install` of a grant that is already installed replaces that installation (idempotent
/// re-install): deploy clients re-send the full plugin list when they cannot compute a plugin
/// delta, e.g. on a config-only redeploy. Installing the same grant twice within one batch is
/// still rejected as a client error.
fn apply_plugin_installation_actions(
    previous: Vec<InstalledPlugin>,
    updates: Vec<PluginInstallationAction>,
    resolved_grants: &HashMap<EnvironmentPluginGrantId, EnvironmentPluginGrantWithDetails>,
) -> Result<Vec<InstalledPlugin>, ComponentError> {
    let mut updated = previous;
    let mut installed_in_batch: HashSet<EnvironmentPluginGrantId> = HashSet::new();

    let position_of = |plugins: &[InstalledPlugin], grant_id: EnvironmentPluginGrantId| {
        plugins
            .iter()
            .position(|p| p.environment_plugin_grant_id == grant_id)
    };

    for update in updates {
        match update {
            PluginInstallationAction::Uninstall(inner) => {
                let grant_id = inner.environment_plugin_grant_id;
                let plugin_index = position_of(&updated, grant_id)
                    .ok_or(ComponentError::PluginInstallationNotFound(grant_id))?;
                updated.swap_remove(plugin_index);
            }
            PluginInstallationAction::Update(inner) => {
                // Currently it's ok to update a plugin even if it was removed from the environment / deleted.
                let grant_id = inner.environment_plugin_grant_id;
                let plugin_index = position_of(&updated, grant_id)
                    .ok_or(ComponentError::PluginInstallationNotFound(grant_id))?;
                let plugin = &mut updated[plugin_index];
                if let Some(new_priority) = inner.new_priority {
                    plugin.priority = new_priority;
                };
                if let Some(new_parameters) = inner.new_parameters {
                    plugin.parameters = new_parameters;
                };
            }
            PluginInstallationAction::Install(inner) => {
                let grant_id = inner.environment_plugin_grant_id;
                if !installed_in_batch.insert(grant_id) {
                    return Err(ComponentError::ConflictingEnvironmentPluginGrantId(
                        grant_id,
                    ));
                }
                // ensure the plugin is (still) granted to the environment
                let grant = resolved_grants
                    .get(&grant_id)
                    .ok_or(ComponentError::EnvironmentPluginNotFound(grant_id))?;
                let plugin = installed_plugin_from_grant(grant, inner.priority, inner.parameters);
                match position_of(&updated, grant_id) {
                    Some(existing_index) => updated[existing_index] = plugin,
                    None => updated.push(plugin),
                }
            }
        }
    }

    let non_unique_priorities = updated
        .iter()
        .into_group_map_by(|p| p.priority)
        .into_iter()
        .filter(|(_, plugins)| plugins.len() > 1)
        .collect::<HashMap<_, _>>();
    if let Some((priority, _)) = non_unique_priorities.iter().next() {
        return Err(ComponentError::ConflictingPluginPriority(*priority));
    }

    Ok(updated)
}

fn validate_and_transform_config_entries(
    agent_type: &AgentType,
    config_entries: Vec<AgentConfigEntryDto>,
) -> Result<Vec<TypedAgentConfigEntry>, ComponentError> {
    validate_agent_config_declarations(agent_type)?;

    let mut results = Vec::new();
    let mut seen_keys = HashSet::new();

    for config_value in config_entries {
        let matching_declaration = agent_type
            .config
            .iter()
            .find(|c| c.path == config_value.path)
            .ok_or_else(|| ComponentError::AgentConfigNotDeclared {
                agent: agent_type.type_name.clone(),
                key: config_value.path.clone(),
            })?;

        if matching_declaration.source == AgentConfigSource::Secret {
            return Err(
                ComponentError::AgentConfigProvidedSecretWhereOnlyLocalAllowed {
                    agent: agent_type.type_name.clone(),
                    path: config_value.path,
                },
            );
        }

        let value =
            ValueAndType::parse_with_type(&config_value.value.0, &matching_declaration.value_type)
                .map_err(|errors| ComponentError::AgentConfigTypeMismatch {
                    agent: agent_type.type_name.clone(),
                    key: config_value.path.clone(),
                    errors,
                })?;

        if !seen_keys.insert(config_value.path.clone()) {
            return Err(ComponentError::AgentConfigDuplicateValue {
                agent: agent_type.type_name.clone(),
                path: config_value.path,
            });
        }

        results.push(TypedAgentConfigEntry {
            path: config_value.path,
            value,
        });
    }

    Ok(results)
}

fn check_config_entries_match(
    agent_type: &AgentType,
    config: &[TypedAgentConfigEntry],
) -> Result<(), ComponentError> {
    validate_agent_config_declarations(agent_type)?;

    for entry in config {
        let matching_declaration = agent_type
            .config
            .iter()
            .find(|c| c.path == entry.path)
            .ok_or_else(|| ComponentError::AgentConfigNotDeclared {
                agent: agent_type.type_name.clone(),
                key: entry.path.clone(),
            })?;

        if matching_declaration.source == AgentConfigSource::Secret {
            return Err(
                ComponentError::AgentConfigProvidedSecretWhereOnlyLocalAllowed {
                    agent: agent_type.type_name.clone(),
                    path: entry.path.clone(),
                },
            );
        };

        if entry.value.typ != matching_declaration.value_type {
            return Err(ComponentError::AgentConfigOldConfigNotValid {
                agent: agent_type.type_name.clone(),
                key: entry.path.clone(),
            });
        }
    }
    Ok(())
}

async fn analyze_and_validate_component_wasm(
    agent_types: Vec<AgentType>,
    wasm: Arc<[u8]>,
    agent_type_provision_configs: BTreeMap<AgentTypeName, AgentTypeProvisionConfig>,
) -> Result<ComponentMetadata, ComponentError> {
    for agent_type in &agent_types {
        validate_agent_config_declarations(agent_type)?;
    }

    let component_metadata = run_cpu_bound_work(move || {
        ComponentMetadata::analyse_component(&wasm, agent_types, agent_type_provision_configs)
    })
    .await?;

    Ok(component_metadata)
}

fn provision_configs_for_agent_types(
    agent_types: &[AgentType],
    provision_configs: BTreeMap<AgentTypeName, AgentTypeProvisionConfig>,
) -> BTreeMap<AgentTypeName, AgentTypeProvisionConfig> {
    let agent_type_names = agent_types
        .iter()
        .map(|agent_type| &agent_type.type_name)
        .collect::<HashSet<_>>();

    provision_configs
        .into_iter()
        .filter(|(agent_type_name, _)| agent_type_names.contains(agent_type_name))
        .collect()
}

fn validate_component_metadata_invariants(
    metadata: &ComponentMetadata,
) -> Result<(), ComponentError> {
    let mut agent_type_names = HashSet::new();

    for agent_type in metadata.agent_types() {
        if !agent_type_names.insert(agent_type.type_name.clone()) {
            return Err(ComponentError::DuplicateAgentTypeName(
                agent_type.type_name.clone(),
            ));
        }
    }

    for agent_type_name in metadata.agent_type_provision_configs().keys() {
        if !agent_type_names.contains(agent_type_name) {
            return Err(ComponentError::UndeclaredAgentTypeInProvisionConfig(
                agent_type_name.clone(),
            ));
        }
    }

    Ok(())
}

fn validate_agent_config_declarations(agent_type: &AgentType) -> Result<(), ComponentError> {
    for declaration in &agent_type.config {
        validate_agent_config_path(&agent_type.type_name, &declaration.path)?;
    }

    Ok(())
}

fn validate_agent_config_path(
    agent: &AgentTypeName,
    path: &[String],
) -> Result<(), ComponentError> {
    if path.iter().any(|segment| segment.contains('.')) {
        return Err(ComponentError::AgentConfigPathSegmentContainsDot {
            agent: agent.clone(),
            key: path.to_vec(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ComponentError, apply_plugin_installation_actions};
    use golem_common::base_model::account::AccountSummary;
    use golem_common::base_model::base64::Base64;
    use golem_common::base_model::environment_plugin_grant::EnvironmentPluginGrantWithDetails;
    use golem_common::base_model::plugin_registration::{
        OplogProcessorPluginSpec, PluginRegistrationDto, PluginSpecDto,
    };
    use golem_common::model::account::AccountId;
    use golem_common::model::component::{
        ComponentId, ComponentRevision, InstalledPlugin, PluginInstallation,
        PluginInstallationAction, PluginInstallationUpdate, PluginPriority,
    };
    use golem_common::model::environment::EnvironmentId;
    use golem_common::model::environment_plugin_grant::EnvironmentPluginGrantId;
    use golem_common::model::plugin_registration::PluginRegistrationId;
    use std::collections::{BTreeMap, HashMap};
    use test_r::test;

    type Grants = HashMap<EnvironmentPluginGrantId, EnvironmentPluginGrantWithDetails>;

    fn grant(name: &str) -> EnvironmentPluginGrantWithDetails {
        let account_id = AccountId::new();
        EnvironmentPluginGrantWithDetails {
            id: EnvironmentPluginGrantId::new(),
            environment_id: EnvironmentId::new(),
            plugin: PluginRegistrationDto {
                id: PluginRegistrationId::new(),
                account_id,
                name: name.to_string(),
                version: "1.0.0".to_string(),
                description: String::new(),
                icon: Base64(Vec::new()),
                homepage: String::new(),
                spec: PluginSpecDto::OplogProcessor(OplogProcessorPluginSpec {
                    component_id: ComponentId::new(),
                    component_revision: ComponentRevision::INITIAL,
                }),
            },
            plugin_account: AccountSummary {
                id: account_id,
                name: "owner".to_string(),
                email: "owner@example.com".into(),
            },
        }
    }

    fn grants(all: &[&EnvironmentPluginGrantWithDetails]) -> Grants {
        all.iter().map(|g| (g.id, (*g).clone())).collect()
    }

    fn installation(
        grant: &EnvironmentPluginGrantWithDetails,
        priority: i32,
    ) -> PluginInstallation {
        PluginInstallation {
            environment_plugin_grant_id: grant.id,
            priority: PluginPriority(priority),
            parameters: BTreeMap::new(),
        }
    }

    fn install(
        grant: &EnvironmentPluginGrantWithDetails,
        priority: i32,
    ) -> PluginInstallationAction {
        PluginInstallationAction::Install(installation(grant, priority))
    }

    fn set_priority(
        grant: &EnvironmentPluginGrantWithDetails,
        priority: i32,
    ) -> PluginInstallationAction {
        PluginInstallationAction::Update(PluginInstallationUpdate {
            environment_plugin_grant_id: grant.id,
            new_priority: Some(PluginPriority(priority)),
            new_parameters: None,
        })
    }

    /// Installs `installs` onto an empty component, i.e. the state after the first deploy.
    fn installed(grants: &Grants, installs: Vec<PluginInstallationAction>) -> Vec<InstalledPlugin> {
        apply_plugin_installation_actions(Vec::new(), installs, grants).unwrap()
    }

    fn priorities_by_name(plugins: &[InstalledPlugin]) -> BTreeMap<String, i32> {
        plugins
            .iter()
            .map(|p| (p.plugin_name.clone(), p.priority.0))
            .collect()
    }

    // Regression (#217): a config-only redeploy re-sends `Install` for every plugin that is
    // already installed (the CLI falls back to install-all when the plugin diff is empty); the
    // existing installation must not collide with itself on its own priority.
    #[test]
    fn reinstalling_already_installed_plugin_on_redeploy_succeeds() {
        let otlp = grant("golem-otlp-exporter");
        let grants = grants(&[&otlp]);
        let previous = installed(&grants, vec![install(&otlp, 0)]);

        let updated =
            apply_plugin_installation_actions(previous, vec![install(&otlp, 0)], &grants).unwrap();

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].environment_plugin_grant_id, otlp.id);
        assert_eq!(updated[0].priority, PluginPriority(0));
    }

    #[test]
    fn reinstalling_already_installed_plugin_replaces_priority_and_parameters() {
        let otlp = grant("golem-otlp-exporter");
        let grants = grants(&[&otlp]);
        let previous = installed(&grants, vec![install(&otlp, 0)]);
        let mut reinstall = installation(&otlp, 5);
        reinstall.parameters = BTreeMap::from([("k".to_string(), "v".to_string())]);

        let updated = apply_plugin_installation_actions(
            previous,
            vec![PluginInstallationAction::Install(reinstall.clone())],
            &grants,
        )
        .unwrap();

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].priority, PluginPriority(5));
        assert_eq!(updated[0].parameters, reinstall.parameters);
    }

    // Regression (#217): updating a plugin to the priority it already has must not collide
    // with itself.
    #[test]
    fn updating_plugin_to_its_own_priority_succeeds() {
        let otlp = grant("golem-otlp-exporter");
        let grants = grants(&[&otlp]);
        let previous = installed(&grants, vec![install(&otlp, 0)]);

        let updated =
            apply_plugin_installation_actions(previous, vec![set_priority(&otlp, 0)], &grants)
                .unwrap();

        assert_eq!(updated[0].priority, PluginPriority(0));
    }

    #[test]
    fn swapping_priorities_within_one_update_succeeds() {
        let a = grant("a");
        let b = grant("b");
        let grants = grants(&[&a, &b]);
        let previous = installed(&grants, vec![install(&a, 0), install(&b, 1)]);

        let updated = apply_plugin_installation_actions(
            previous,
            vec![set_priority(&a, 1), set_priority(&b, 0)],
            &grants,
        )
        .unwrap();

        assert_eq!(
            priorities_by_name(&updated),
            BTreeMap::from([("a".to_string(), 1), ("b".to_string(), 0)])
        );
    }

    #[test]
    fn installing_different_plugin_with_taken_priority_is_rejected() {
        let a = grant("a");
        let b = grant("b");
        let grants = grants(&[&a, &b]);
        let previous = installed(&grants, vec![install(&a, 0)]);

        let result = apply_plugin_installation_actions(previous, vec![install(&b, 0)], &grants);

        assert!(matches!(
            result,
            Err(ComponentError::ConflictingPluginPriority(PluginPriority(0)))
        ));
    }

    #[test]
    fn updating_plugin_to_another_plugins_priority_is_rejected() {
        let a = grant("a");
        let b = grant("b");
        let grants = grants(&[&a, &b]);
        let previous = installed(&grants, vec![install(&a, 0), install(&b, 1)]);

        let result =
            apply_plugin_installation_actions(previous, vec![set_priority(&b, 0)], &grants);

        assert!(matches!(
            result,
            Err(ComponentError::ConflictingPluginPriority(PluginPriority(0)))
        ));
    }

    #[test]
    fn installing_same_plugin_twice_in_one_update_is_rejected() {
        let a = grant("a");
        let grants = grants(&[&a]);

        let result = apply_plugin_installation_actions(
            Vec::new(),
            vec![install(&a, 0), install(&a, 1)],
            &grants,
        );

        assert!(matches!(
            result,
            Err(ComponentError::ConflictingEnvironmentPluginGrantId(id)) if id == a.id
        ));
    }
}
