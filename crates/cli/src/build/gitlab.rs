//! This module defines some types used to represent the information collected
//! from GitLab for each of the landscape items repositories (when applicable),
//! as well as the functionality used to collect that information.

use std::collections::BTreeMap;
use std::env;
use std::sync::LazyLock;

use anyhow::{Result, format_err};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool::unmanaged::{Object, Pool};
use reqwest::header::{HeaderMap, HeaderValue};
use futures::stream::{self, StreamExt};
use gitlab::api::{self, AsyncQuery, Pagination};
use gitlab::api::common::SortOrder;
use gitlab::api::projects::Project;
use gitlab::api::projects::releases::ProjectReleases;
use gitlab::api::projects::repository::commits::Commits;
use gitlab::api::projects::repository::contributors::Contributors;
use gitlab::{AsyncGitlab, Gitlab};
use landscape2_core::data::{Commit, Contributors as DataContributors, GitData, RepositoryGitData};
#[cfg(test)]
use mockall::automock;
use regex::Regex;
use serde::Deserialize;
use tracing::{debug, instrument, warn};

use super::{LandscapeData, cache::Cache};

/// File used to cache data collected from GitLab.
const GITLAB_CACHE_FILE: &str = "gitlab.json";

/// How long the GitLab data in the cache is valid (in days).
const GITLAB_CACHE_TTL: i64 = 7;

/// Environment variable containing GitLab tokens configuration.
/// Format: "token1,token2" for gitlab.com or "url1;token1;url2;token2" for multiple instances
const GITLAB_TOKENS: &str = "GITLAB_TOKENS";

/// Default GitLab instance URL.
const DEFAULT_GITLAB_URL: &str = "https://gitlab.com";

/// Configuration for a GitLab instance.
#[derive(Debug, Clone)]
struct GitlabInstanceConfig {
    base_url: String,
    tokens: Vec<String>,
}

/// Collect GitLab data for each of the items repositories in the landscape,
/// reusing cached data whenever possible.
#[instrument(skip_all, err)]
pub(crate) async fn collect_gitlab_data(cache: &Cache, landscape_data: &LandscapeData) -> Result<GitData> {
    debug!("collecting repositories information from gitlab (this may take a while)");
    
    // Collect GitLab repository URLs and group them by instance
    let mut repos_by_instance: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for item in &landscape_data.items {
        if let Some(repositories) = &item.repositories {
            for repo in repositories {
                if let Some((base_url, _path)) = parse_gitlab_url(&repo.url) {
                    repos_by_instance
                        .entry(base_url)
                        .or_default()
                        .push(&repo.url);
                }
            }
        }
    }

    debug!("found {} GitLab instances with repositories: {:?}", repos_by_instance.len(), repos_by_instance.keys().collect::<Vec<_>>());

    // Early return if no GitLab repositories found
    if repos_by_instance.is_empty() {
        debug!("no gitlab repositories found");
        return Ok(BTreeMap::new());
    }

    // Read cached data (if available)
    let mut cached_data: Option<GitData> = None;
    match cache.read(GITLAB_CACHE_FILE) {
        Ok(Some((_, json_data))) => match serde_json::from_slice(&json_data) {
            Ok(gitlab_data) => cached_data = Some(gitlab_data),
            Err(err) => warn!("error parsing gitlab cache file: {err:?}"),
        },
        Ok(None) => {}
        Err(err) => warn!("error reading gitlab cache file: {err:?}"),
    }

    // Parse GitLab tokens configuration
    let instance_configs = parse_gitlab_tokens_env()?;

    // Remove duplicates
    for urls in repos_by_instance.values_mut() {
        urls.sort();
        urls.dedup();
    }

    // Create client pools for each instance that has repositories
    let mut instance_pools: BTreeMap<String, Pool<DynGL>> = BTreeMap::new();
    for (base_url, repo_urls) in &repos_by_instance {
        if let Some(config) = find_config_for_instance(base_url, &instance_configs) {
            let gl_pool = create_gitlab_pool(base_url, &config.tokens).await?;
            instance_pools.insert(base_url.clone(), gl_pool);
        } else {
            warn!("no gitlab token configured for instance: {base_url} ({} repositories will be skipped)", repo_urls.len());
        }
    }

    if instance_pools.is_empty() {
        warn!("gitlab tokens not provided: no information will be collected from gitlab");
        return Ok(BTreeMap::new());
    }

    // Collect repositories information from GitLab, reusing cached data when available
    let mut all_urls = vec![];
    for urls in repos_by_instance.values() {
        all_urls.extend(urls.iter().copied());
    }

    debug!("collecting data for {} gitlab repositories", all_urls.len());

    let total_tokens: usize = instance_configs.iter().map(|c| c.tokens.len()).sum();
    let concurrency = total_tokens.max(1);

    let gitlab_data: GitData = stream::iter(all_urls)
        .map(|url| async {
            let url = url.to_string();

            // Use cached data when available if it hasn't expired yet
            if let Some(cached_repo) = cached_data.as_ref().and_then(|cache| {
                cache.get(&url).and_then(|repo| {
                    if repo.generated_at + chrono::Duration::days(GITLAB_CACHE_TTL) > Utc::now() {
                        Some(repo)
                    } else {
                        None
                    }
                })
            }) {
                debug!("using cached data for {}", url);
                (url, Ok(cached_repo.clone()))
            }
            // Otherwise we pull it from GitLab if a pool exists for this instance
            else if let Some((base_url, _)) = parse_gitlab_url(&url) {
                if let Some(gl_pool) = instance_pools.get(&base_url) {
                    debug!("fetching fresh data for {}", url);
                    let gl = gl_pool.get().await.expect("token -when available-");
                    (url.clone(), collect_repository_data(gl, &url).await)
                } else {
                    (url.clone(), Err(format_err!("no token configured for instance")))
                }
            } else {
                (url.clone(), Err(format_err!("invalid gitlab url")))
            }
        })
        .buffer_unordered(concurrency)
        .collect::<BTreeMap<String, Result<RepositoryGitData>>>()
        .await
        .into_iter()
        .filter_map(|(url, result)| {
            if let Ok(gitlab_data) = result {
                Some((url, gitlab_data))
            } else {
                None
            }
        })
        .collect();

    // Write data (in json format) to cache
    cache.write(GITLAB_CACHE_FILE, &serde_json::to_vec_pretty(&gitlab_data)?)?;

    debug!("collected data for {} gitlab repositories", gitlab_data.len());
    debug!("done!");

    Ok(gitlab_data)
}

/// Parse GitLab tokens from environment variable.
fn parse_gitlab_tokens_env() -> Result<Vec<GitlabInstanceConfig>> {
    let tokens_env = match env::var(GITLAB_TOKENS) {
        Ok(t) if !t.is_empty() => t,
        _ => return Ok(vec![]),
    };

    let mut configs = vec![];

    // Split by semicolon for different instances/tokens
    let parts: Vec<&str> = tokens_env.split(';').collect();
    
    let mut i = 0;
    while i < parts.len() {
        let part = parts[i].trim();
        if part.is_empty() {
            i += 1;
            continue;
        }

        // Check if this part looks like a URL (starts with http:// or https://)
        if part.starts_with("http://") || part.starts_with("https://") {
            // Next part should be the token(s)
            if i + 1 < parts.len() {
                let tokens_part = parts[i + 1].trim();
                let tokens: Vec<String> = tokens_part
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if !tokens.is_empty() {
                    let base_url = part.trim_end_matches('/').to_string();
                    configs.push(GitlabInstanceConfig {
                        base_url,
                        tokens,
                    });
                }
                
                i += 2; // Skip both URL and token parts
                continue;
            } else {
                i += 1;
                continue;
            }
        }

        // No URL prefix - tokens for default gitlab.com
        let tokens: Vec<String> = part
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if !tokens.is_empty() {
            configs.push(GitlabInstanceConfig {
                base_url: DEFAULT_GITLAB_URL.to_string(),
                tokens,
            });
        }
        
        i += 1;
    }

    Ok(configs)
}

/// Find the configuration for a given GitLab instance.
fn find_config_for_instance<'a>(
    base_url: &str,
    configs: &'a [GitlabInstanceConfig],
) -> Option<&'a GitlabInstanceConfig> {
    let normalized_url = base_url.trim_end_matches('/').to_lowercase();
    configs
        .iter()
        .find(|c| c.base_url.trim_end_matches('/').to_lowercase() == normalized_url)
}

/// Create a pool of GitLab API clients for the given instance.
async fn create_gitlab_pool(base_url: &str, tokens: &[String]) -> Result<Pool<DynGL>> {
    let mut gl_clients: Vec<DynGL> = vec![];
    for token in tokens {
        let gl = Box::new(GLApi::new(base_url, token).await?);
        gl_clients.push(gl);
    }
    Ok(Pool::from(gl_clients))
}

/// Collect repository data from GitLab.
#[instrument(skip_all, err)]
async fn collect_repository_data(gl: Object<DynGL>, repo_url: &str) -> Result<RepositoryGitData> {
    let (base_url, path) = parse_gitlab_url(repo_url)
        .ok_or_else(|| format_err!("invalid gitlab repository url"))?;

    let gl_project = gl.get_project(&path).await?;
    collect_project_data(&gl, &base_url, &path, gl_project).await
}

/// Collect data for a GitLab project.
async fn collect_project_data(
    gl: &Object<DynGL>,
    base_url: &str,
    project_path: &str,
    gl_project: GitLabProject,
) -> Result<RepositoryGitData> {
    let contributors_count = gl.get_contributors_count(project_path).await?;
    let first_commit = gl.get_first_commit(project_path, &gl_project.default_branch).await?;
    
    debug!("collecting languages for {}", project_path);
    let languages = gl.get_languages(project_path).await?;
    debug!("languages result for {}: {:?}", project_path, languages);
    
    let good_first_issues = gl.get_good_first_issues_count(project_path).await?;
    
    let latest_commit = gl.get_latest_commit(project_path, &gl_project.default_branch).await?;
    let latest_release = gl.get_latest_release(project_path).await?;

    // Prepare repository instance using the information collected
    Ok(RepositoryGitData {
        generated_at: Utc::now(),
        contributors: DataContributors {
            count: contributors_count,
            url: format!("{base_url}/{project_path}/-/graphs/main?ref_type=heads"),
        },
        description: gl_project.description.unwrap_or_default(),
        first_commit,
        good_first_issues,
        languages,
        latest_commit,
        latest_release,
        license: gl_project.license.map(|l| l.name),
        stars: gl_project.star_count,
        topics: gl_project.topics,
        url: gl_project.web_url,
        ..Default::default()
    })
}

/// Type alias to represent a GL trait object.
type DynGL = Box<dyn GL + Send + Sync>;

/// Trait that defines some operations a GL implementation must support.
#[async_trait]
#[cfg_attr(test, automock)]
trait GL {
    /// Get number of repository contributors.
    async fn get_contributors_count(&self, project_path: &str) -> Result<usize>;

    /// Get first commit.
    async fn get_first_commit(&self, project_path: &str, ref_: &str) -> Result<Option<Commit>>;

    /// Get count of good first issues.
    async fn get_good_first_issues_count(&self, project_path: &str) -> Result<Option<usize>>;

    /// Get languages used in repository.
    async fn get_languages(&self, project_path: &str) -> Result<Option<BTreeMap<String, i64>>>;

    /// Get latest commit.
    async fn get_latest_commit(&self, project_path: &str, ref_: &str) -> Result<Commit>;

    /// Get latest release.
    async fn get_latest_release(&self, project_path: &str) -> Result<Option<landscape2_core::data::Release>>;

    /// Get project.
    async fn get_project(&self, project_path: &str) -> Result<GitLabProject>;
}

/// GH implementation backed by the GitLab API.
struct GLApi {
    base_url: String,
    client: AsyncGitlab,
    http_client: reqwest::Client,
}

impl GLApi {
    /// Create a new GLApi instance.
    async fn new(base_url: &str, token: &str) -> Result<Self> {
        // Strip protocol from base_url if present - gitlab crate adds it automatically
        let host = base_url
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        
        let client = Gitlab::builder(host, token)
            .build_async()
            .await?;

        // Setup HTTP client for direct API calls
        let mut headers = HeaderMap::new();
        headers.insert(
            "PRIVATE-TOKEN",
            HeaderValue::from_str(token)?
        );
        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            base_url: base_url.to_string(),
            client,
            http_client,
        })
    }
}

#[async_trait]
impl GL for GLApi {
    /// [GL::get_contributors_count]
    #[instrument(skip(self), err)]
    async fn get_contributors_count(&self, project_path: &str) -> Result<usize> {
        let endpoint = Contributors::builder()
            .project(project_path)
            .build()?;

        let contributors: Vec<GitLabContributor> = api::paged(endpoint, Pagination::All)
            .query_async(&self.client)
            .await?;

        debug!("GitLab Contributors Response for {}: {:?}", project_path, contributors);

        Ok(contributors.len())
    }

    /// [GL::get_first_commit]
    #[instrument(skip(self), err)]
    async fn get_first_commit(&self, project_path: &str, ref_: &str) -> Result<Option<Commit>> {
        // Get commits ordered from oldest to newest
        let endpoint = Commits::builder()
            .project(project_path)
            .ref_name(ref_)
            .build()?;

        let mut commits: Vec<GitLabCommit> = api::paged(endpoint, Pagination::All)
            .query_async(&self.client)
            .await?;

        // Get the last commit (oldest)
        if let Some(commit) = commits.pop() {
            return Ok(Some(Commit {
                url: commit.web_url,
                ts: Some(commit.committed_date),
            }));
        }

        Ok(None)
    }

    /// [GL::get_good_first_issues_count]
    #[instrument(skip(self), err)]
    async fn get_good_first_issues_count(&self, project_path: &str) -> Result<Option<usize>> {
        let encoded_path = urlencoding::encode(project_path);
        let url = format!(
            "{}/api/v4/projects/{}/issues_statistics?labels=good first issue&state=opened",
            self.base_url, encoded_path
        );
        
        debug!("Fetching good first issues count for {} from URL: {}", project_path, url);
        
        let response = self.http_client.get(&url).send().await?;
        
        if !response.status().is_success() {
            debug!("Failed to get good first issues count for {}: status {}", project_path, response.status());
            return Ok(None);
        }
        
        let response_text = response.text().await?;
        debug!("Good first issues API response for {}: {}", project_path, response_text);
        
        #[derive(Deserialize)]
        struct IssuesStatistics {
            statistics: Statistics,
        }
        
        #[derive(Deserialize)]
        struct Statistics {
            counts: Counts,
        }
        
        #[derive(Deserialize)]
        struct Counts {
            opened: usize,
        }
        
        match serde_json::from_str::<IssuesStatistics>(&response_text) {
            Ok(stats) => {
                debug!("Good first issues count for {}: {}", project_path, stats.statistics.counts.opened);
                Ok(Some(stats.statistics.counts.opened))
            }
            Err(e) => {
                debug!("Failed to parse good first issues response for {}: {}", project_path, e);
                Ok(None)
            }
        }
    }

    /// [GL::get_languages]
    #[instrument(skip(self), err)]
    async fn get_languages(&self, project_path: &str) -> Result<Option<BTreeMap<String, i64>>> {
        let encoded_path = urlencoding::encode(project_path);
        let url = format!("{}/api/v4/projects/{}/languages", self.base_url, encoded_path);
        
        debug!("Fetching languages for {} from URL: {}", project_path, url);
        
        let response = self.http_client.get(&url).send().await?;
        
        debug!("Languages API response status for {}: {}", project_path, response.status());
        
        if !response.status().is_success() {
            warn!("failed to get languages for {}: status {}", project_path, response.status());
            return Ok(None);
        }
        
        // Get raw response text for debugging
        let response_text = response.text().await?;
        debug!("Languages raw API response for {}: {}", project_path, response_text);
        
        // GitLab returns percentages as floats
        let languages: BTreeMap<String, f64> = serde_json::from_str(&response_text)?;
        
        debug!("Languages parsed response for {}: {:?}", project_path, languages);
        
        if languages.is_empty() {
            debug!("No languages found for {}", project_path);
            return Ok(None);
        }
        
        // Convert percentages to approximate byte counts (normalize to 100000 total)
        let lang_counts: BTreeMap<String, i64> = languages
            .into_iter()
            .map(|(lang, percentage)| (lang, (percentage * 1000.0) as i64))
            .collect();
        
        debug!("Languages converted for {}: {:?}", project_path, lang_counts);
        
        Ok(Some(lang_counts))
    }

    /// [GL::get_latest_commit]
    #[instrument(skip(self), err)]
    async fn get_latest_commit(&self, project_path: &str, ref_: &str) -> Result<Commit> {
        let endpoint = Commits::builder()
            .project(project_path)
            .ref_name(ref_)
            .build()?;

        let commits: Vec<GitLabCommit> = api::paged(endpoint, Pagination::Limit(1))
            .query_async(&self.client)
            .await?;

        let commit = commits
            .first()
            .ok_or_else(|| format_err!("no commits found"))?;

        Ok(Commit {
            url: commit.web_url.clone(),
            ts: Some(commit.committed_date),
        })
    }

    /// [GL::get_latest_release]
    #[instrument(skip(self), err)]
    async fn get_latest_release(&self, project_path: &str) -> Result<Option<landscape2_core::data::Release>> {
        let endpoint = ProjectReleases::builder()
            .project(project_path)
            .sort(SortOrder::Descending)
            .build()?;

        let releases: Vec<GitLabRelease> = api::paged(endpoint, Pagination::Limit(1))
            .query_async(&self.client)
            .await?;

        if let Some(release) = releases.first() {
            let ts = release.released_at.or(release.created_at);
            let url = release.links.self_link.clone().unwrap_or_else(|| {
                format!("{}/{project_path}/-/releases", self.base_url)
            });
            
            Ok(Some(landscape2_core::data::Release { ts, url }))
        } else {
            Ok(None)
        }
    }

    /// [GL::get_project]
    #[instrument(skip(self), err)]
    async fn get_project(&self, project_path: &str) -> Result<GitLabProject> {
        let endpoint = Project::builder()
            .project(project_path)
            .license(true)
            .build()?;

        let project: GitLabProject = endpoint.query_async(&self.client).await?;
        
        debug!("Project response for {}: description={:?}, license={:?}, topics={:?}", 
               project_path, 
               project.description.as_ref().map(|s| &s[..s.len().min(50)]),
               project.license,
               project.topics);
        
        Ok(project)
    }
}

/// GitLab repository url regular expression.
pub(crate) static GITLAB_REPO_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?P<base>https://[^/]+)/(?P<path>.+?)/?$")
        .expect("exprs in GITLAB_REPO_URL to be valid")
});

/// Parse GitLab URL to extract base URL and project path.
fn parse_gitlab_url(repo_url: &str) -> Option<(String, String)> {
    // Skip GitHub URLs
    if repo_url.contains("github.com") {
        return None;
    }

    GITLAB_REPO_URL.captures(repo_url).map(|c| {
        let base = c["base"].to_string();
        let path = c["path"].trim_end_matches(".git").to_string();
        (base, path)
    })
}

/// GitLab project information returned by the API.
#[derive(Debug, Clone, Deserialize)]
struct GitLabProject {
    #[serde(default)]
    pub description: Option<String>,
    pub default_branch: String,
    pub path_with_namespace: String,
    pub star_count: i64,
    #[serde(default)]
    pub topics: Vec<String>,
    pub web_url: String,
    #[serde(default)]
    pub license: Option<GitLabLicense>,
}

/// GitLab license information.
#[derive(Debug, Clone, Deserialize)]
struct GitLabLicense {
    pub name: String,
}

/// GitLab contributor information.
#[derive(Debug, Clone, Deserialize)]
struct GitLabContributor {
    #[allow(dead_code)]
    pub name: String,
}

/// GitLab commit information.
#[derive(Debug, Clone, Deserialize)]
struct GitLabCommit {
    pub web_url: String,
    pub committed_date: DateTime<Utc>,
}

/// GitLab release information.
#[derive(Debug, Clone, Deserialize)]
struct GitLabRelease {
    pub released_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    #[serde(rename = "_links")]
    pub links: GitLabReleaseLinks,
}

/// GitLab release links.
#[derive(Debug, Clone, Deserialize)]
struct GitLabReleaseLinks {
    #[serde(rename = "self")]
    pub self_link: Option<String>,
}
