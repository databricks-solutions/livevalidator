/**
 * Validation utilities
 */

export function validateRequired(value, fieldName) {
  if (!value || value.trim() === '') {
    return `${fieldName} is required`;
  }
  return null;
}

export function validateNumber(value, fieldName) {
  if (value !== undefined && value !== null && value !== '') {
    const num = Number(value);
    if (isNaN(num)) {
      return `${fieldName} must be a valid number`;
    }
  }
  return null;
}

export function parseArrayField(value) {
  if (!value) return [];
  if (typeof value === 'string') {
    return value.split(',').map(s => s.trim()).filter(s => s);
  }
  return Array.isArray(value) ? value : [];
}

