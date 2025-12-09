import isUndefined from 'lodash/isUndefined';

import { Item, Repository } from '../types/types';
import { cleanEmojis } from './cleanEmojis';

// Get item's description
export const getItemDescription = (item?: Item | null): string => {
  if (item) {
    if (item.description && item.description !== '') {
      return item.description;
    } else if (item.repositories) {
      const primaryRepo = item.repositories.find((repo: Repository) => repo.primary);
      if (
        primaryRepo &&
        primaryRepo.git_data &&
        primaryRepo.git_data.description &&
        primaryRepo.git_data.description !== ''
      ) {
        return cleanEmojis(primaryRepo.git_data.description);
      }
    } else if (
      isUndefined(item.maturity) &&
      item.crunchbase_data &&
      item.crunchbase_data.description &&
      item.crunchbase_data.description !== ''
    ) {
      return cleanEmojis(item.crunchbase_data.description);
    }
  }

  return 'This item does not have a description available yet';
};
