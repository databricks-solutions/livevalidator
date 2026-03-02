/**
 * Safely parse arrays that may be JSON strings or already arrays.
 * Handles tags and other array fields from the backend.
 */
export function parseArray(arr) {
  if (!arr) return [];
  if (Array.isArray(arr)) return arr;
  if (typeof arr === 'string') {
    try {
      const parsed = JSON.parse(arr);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}
