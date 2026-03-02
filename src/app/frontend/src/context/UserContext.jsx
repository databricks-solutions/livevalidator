import { createContext, useContext } from 'react';

export const CurrentUserContext = createContext(null);
export const useCurrentUser = () => useContext(CurrentUserContext);

export const canCreate = (role) => {
  if (!role) return false;
  return ['CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE'].includes(role);
};

export const canRun = (role) => {
  if (!role) return false;
  return ['CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE'].includes(role);
};

export const canEdit = (role, createdBy, currentUserEmail) => {
  if (!role) return false;
  if (role === 'CAN_VIEW') return false;
  if (['CAN_EDIT', 'CAN_MANAGE'].includes(role)) return true;
  return role === 'CAN_RUN' && createdBy === currentUserEmail;
};

export const canManageSystems = (role) => {
  return role === 'CAN_MANAGE';
};
