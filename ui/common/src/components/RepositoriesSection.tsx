import isEmpty from 'lodash/isEmpty';
import isUndefined from 'lodash/isUndefined';
import orderBy from 'lodash/orderBy';
import moment from 'moment';
import { createSignal, For, onMount, Show } from 'solid-js';
import { css } from 'solid-styled-components';

import { Repository, SVGIconKind } from '../types/types';
import { prettifyNumber } from '../utils/prettifyNumber';
import { Box } from './Box';
import { ExternalLink } from './ExternalLink';
import { LanguagesStats } from './LanguagesStats';
import { ParticipationStats } from './ParticipationStats';
import { SVGIcon } from './SVGIcon';

interface Props {
  repositories?: Repository[];
  class?: string;
  titleClass?: string;
  boxClass?: string;
}

interface RepoProps {
  repository: Repository;
  boxClass?: string;
}

const SubtitleInSection = css`
  font-size: 0.8rem !important;
  opacity: 0.5;
`;

const BadgeOutlineDark = css`
  border: 1px solid var(--bs-gray-700);
  color: var(--bs-gray-700) !important;
`;

const MiniBadge = css`
  font-size: 0.65rem !important;
`;

const Select = css`
  width: 500px !important;
  max-width: calc(100% - 0.4rem);
  box-shadow: 0 0 0 0.2rem var(--bs-gray-200);
  margin: 0 0.2rem;
  font-size: 0.8rem !important;

  &:focus {
    box-shadow: 0 0 0 0.2rem var(--bs-gray-200) !important;
  }
`;

const ExternalIcon = css`
  min-width: 24px;
`;

const RepoIcon = css`
  min-width: 24px;
`;

const LinkWrapper = css`
  max-width: 100%;
`;

const TruncateWrapper = css`
  min-width: 0;
  max-width: 100%;
`;

const LinkContentWrapper = css`
  max-width: calc(100% - 1.5rem);
`;

const GoodFirstBadge = css`
  height: 20px;

  img {
    max-height: 100%;
  }
`;

const Badges = css`
  row-gap: 0.5rem;
`;

const formatRepoUrl = (url: string): string => {
  const repoUrl = new URL(url);
  return repoUrl.pathname.slice(1);
};

const isGitLabRepo = (repo: Repository): boolean => {
  // Check if URL contains gitlab
  return repo.url.includes('gitlab');
};

const getGoodFirstIssueUrl = (repo: Repository): string => {
  if (isGitLabRepo(repo)) {
    return `${repo.url}/-/issues?state=opened&label_name[]=good first issue`;
  }
  return `${repo.url}/issues?q=is%3Aopen+is%3Aissue+label%3A"good+first+issue"`;
};

const getGoodFirstIssueBadgeUrl = (repo: Repository): string | undefined => {
  // Only GitHub has shields.io badge support
  if (isGitLabRepo(repo)) {
    return undefined;
  }
  return `https://img.shields.io/github/issues/${formatRepoUrl(
    repo.url
  )}/good%20first%20issue.svg?style=flat-square&label=good%20first%20issues&labelColor=e9ecef&color=6c757d`;
};

const RepositoryInfo = (props: RepoProps) => {
  const formatDate = (date: string): string => {
    return moment(date).format("MMM 'YY");
  };

  const isGitLab = (): boolean => {
    return isGitLabRepo(props.repository);
  };

  return (
    <>
      <div class="d-flex flex-row align-items-start mt-4">
        <div class={`d-flex flex-column flex-md-row align-items-start align-items-md-center my-2 ${LinkWrapper}`}>
          <ExternalLink
            class={`text-reset p-0 align-items-center fw-semibold me-3 text-decoration-none ${TruncateWrapper}`}
            href={props.repository.url}
            externalIconClassName={ExternalIcon}
            visibleExternalIcon
          >
            <div class={`d-none d-md-flex ${LinkContentWrapper}`}>
              <div class="text-truncate">{props.repository.url}</div>
            </div>
            <div class="d-flex d-md-none flex-row align-items-center text-truncate">
              <SVGIcon kind={isGitLab() ? SVGIconKind.GitLab : SVGIconKind.GitHub} class={`me-1 ${RepoIcon}`} />
              <div class="text-truncate">{formatRepoUrl(props.repository.url)}</div>
            </div>
          </ExternalLink>
          <Show when={props.repository.primary || !isUndefined(props.repository.git_data)}>
            <div class={`d-flex align-items-center flex-wrap flex-md-nowrap mt-2 mt-md-0 ${Badges}`}>
              <Show when={props.repository.primary}>
                <div class={`me-2 badge rounded-0 text-uppercase ${BadgeOutlineDark} ${MiniBadge}`}>Primary</div>
              </Show>
              <Show when={!isUndefined(props.repository.license) || !isUndefined(props.repository.git_data)}>
                <div class={`badge rounded-0 me-2 ${BadgeOutlineDark} ${MiniBadge}`}>
                  {props.repository.license || props.repository.git_data?.license}
                </div>
              </Show>
              <div class="d-none d-md-flex">
                <Show when={getGoodFirstIssueBadgeUrl(props.repository)}>
                  <ExternalLink class={`d-flex ${GoodFirstBadge}`} href={getGoodFirstIssueUrl(props.repository)}>
                    <img src={getGoodFirstIssueBadgeUrl(props.repository)!} />
                  </ExternalLink>
                </Show>
                <Show when={!getGoodFirstIssueBadgeUrl(props.repository)}>
                  <ExternalLink
                    class={`badge rounded-0 text-decoration-none ${BadgeOutlineDark} ${MiniBadge}`}
                    href={getGoodFirstIssueUrl(props.repository)}
                  >
                    <Show when={!isUndefined(props.repository.git_data?.good_first_issues)}>
                      {props.repository.git_data!.good_first_issues} good first issue{props.repository.git_data!.good_first_issues !== 1 ? 's' : ''}
                    </Show>
                    <Show when={isUndefined(props.repository.git_data?.good_first_issues)}>
                      Good first issues
                    </Show>
                  </ExternalLink>
                </Show>
              </div>
            </div>
          </Show>
        </div>
      </div>
      <Show when={!isUndefined(props.repository.git_data)}>
        <div class="row g-4 my-0 mb-2 justify-content-center justify-md-content-start">
          <Box
            class={props.boxClass}
            value={prettifyNumber(props.repository.git_data!.stars, 1)}
            legend="Stars"
            description="Stars number"
          />

          <Box
            class={props.boxClass}
            value={prettifyNumber(props.repository.git_data!.contributors.count)}
            legend="Contributors"
            description="Contributors number"
          />

          <Show when={!isUndefined(props.repository.git_data!.first_commit)}>
            <Box
              class={props.boxClass}
              value={formatDate(props.repository.git_data!.first_commit!.ts)}
              legend="First commit"
              description="First commit date"
            />
          </Show>

          <Box
            class={props.boxClass}
            value={formatDate(props.repository.git_data!.latest_commit.ts)}
            legend="Latest commit"
            description="Latest commit date"
          />

          <Box
            class={props.boxClass}
            value={
              !isUndefined(props.repository.git_data!.latest_release)
                ? formatDate(props.repository.git_data!.latest_release!.ts)
                : '-'
            }
            legend="Latest release"
            description="Latest release date"
          />
        </div>

        <Show when={!isUndefined(props.repository.git_data!.participation_stats)}>
          <div class="mt-4">
            <div class={`fw-semibold ${SubtitleInSection}`}>Participation stats</div>
            <div class="mx-2 mx-md-0">
              <ParticipationStats initialStats={props.repository.git_data!.participation_stats!} />
            </div>
          </div>
        </Show>

        <Show
          when={
            !isUndefined(props.repository.git_data!.languages) && !isEmpty(props.repository.git_data!.languages)
          }
        >
          <div class="mt-4">
            <div class={`fw-semibold ${SubtitleInSection}`}>Languages</div>
            <LanguagesStats initialLanguages={props.repository.git_data!.languages!} boxClass={props.boxClass} />
          </div>
        </Show>
      </Show>
    </>
  );
};

export const RepositoriesSection = (props: Props) => {
  const [selectedRepo, setSelectedRepo] = createSignal<Repository | undefined>(undefined);
  const [repositoriesList, setRepositoriesList] = createSignal<Repository[]>([]);

  onMount(() => {
    if (props.repositories && props.repositories.length > 0) {
      const sortedRepos = orderBy(
        props.repositories,
        [(r: Repository) => (r.primary === true ? 0 : 1), 'url'],
        ['asc']
      );
      setRepositoriesList(sortedRepos);
      setSelectedRepo(sortedRepos[0]);
    }
  });

  return (
    <Show when={repositoriesList().length > 0}>
      <div class={`position-relative ${props.class}`}>
        <div class={` ${props.titleClass}`}>Repositories</div>
        <select
          id="repo-select"
          class={`form-select form-select-md border-0 rounded-0 my-3 ${Select}`}
          value={selectedRepo() ? selectedRepo()!.url : undefined}
          aria-label="Repositories select"
          onChange={(e) => {
            const repo = repositoriesList().find((r) => r.url === e.currentTarget.value);
            setSelectedRepo(repo);
          }}
        >
          <For each={repositoriesList()}>
            {(repo: Repository) => {
              return (
                <option value={repo.url}>
                  {formatRepoUrl(repo.url)}
                  <Show when={repo.primary}> (primary)</Show>
                </option>
              );
            }}
          </For>
        </select>

        <Show when={!isUndefined(selectedRepo())}>
          <RepositoryInfo repository={selectedRepo()!} boxClass={props.boxClass} />
        </Show>
      </div>
    </Show>
  );
};
