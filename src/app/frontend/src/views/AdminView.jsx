import React, { useState, useEffect } from 'react';
import { useCurrentUser } from '../App';
import { API } from '../services/api';

export function AdminView() {
  const currentUser = useCurrentUser();
  const [users, setUsers] = useState([]);
  const [config, setConfig] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [saving, setSaving] = useState(false);
  const [notification, setNotification] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'user_email', direction: 'asc' });

  // Check permission
  if (!currentUser || currentUser.role !== 'CAN_MANAGE') {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="bg-red-900/20 border-2 border-red-700 rounded-xl p-8 text-red-200 max-w-md shadow-2xl">
          <div className="text-center">
            <h3 className="text-xl font-bold mb-3">Access Denied</h3>
            <p className="text-red-300 leading-relaxed">Only users with CAN_MANAGE role can access this page.</p>
          </div>
        </div>
      </div>
    );
  }

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);
      const [usersData, configData] = await Promise.all([
        API.userRoles.listUsers(),
        API.admin.getConfig()
      ]);
      setUsers(usersData);
      
      // Convert config array to object
      const configObj = {};
      configData.forEach(item => {
        configObj[item.key] = item.value;
      });
      setConfig(configObj);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const updateUserRole = async (userEmail, newRole) => {
    try {
      await API.userRoles.setUserRole(userEmail, newRole);
      setNotification({ type: 'success', message: `Updated ${userEmail} to ${newRole}` });
      setTimeout(() => setNotification(null), 3000);
      loadData();
    } catch (err) {
      setNotification({ type: 'error', message: `Failed: ${err.message}` });
      setTimeout(() => setNotification(null), 5000);
    }
  };

  const deleteUser = async (userEmail) => {
    console.log('[AdminView] Delete clicked for:', userEmail);
    
    const confirmed = window.confirm(`Remove ${userEmail} from the system?\n\nThey will be re-added with the default role if they access the app again.`);
    console.log('[AdminView] Confirmation result:', confirmed);
    
    if (!confirmed) {
      return;
    }
    
    try {
      console.log('[AdminView] Calling delete API...');
      const result = await API.userRoles.deleteUserRole(userEmail);
      console.log('[AdminView] Delete result:', result);
      
      setNotification({ type: 'success', message: `Removed ${userEmail}` });
      setTimeout(() => setNotification(null), 3000);
      
      console.log('[AdminView] Reloading data...');
      await loadData();
    } catch (err) {
      console.error('[AdminView] Delete error:', err);
      setNotification({ type: 'error', message: `Failed: ${err.message}` });
      setTimeout(() => setNotification(null), 5000);
    }
  };

  const saveDefaultRole = async () => {
    try {
      setSaving(true);
      await API.admin.updateConfig('default_user_role', config.default_user_role);
      setNotification({ type: 'success', message: 'Configuration saved successfully' });
      setTimeout(() => setNotification(null), 3000);
    } catch (err) {
      setNotification({ type: 'error', message: `Failed: ${err.message}` });
      setTimeout(() => setNotification(null), 5000);
    } finally {
      setSaving(false);
    }
  };

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  // Filter and sort users
  const filteredAndSortedUsers = [...users]
    .filter(user => 
      user.user_email.toLowerCase().includes(searchQuery.toLowerCase()) ||
      user.role.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      const aVal = String(a[sortConfig.key] || '');
      const bVal = String(b[sortConfig.key] || '');
      
      const comparison = aVal.localeCompare(bVal);
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });

  const getRoleBadgeColor = (role) => {
    switch (role) {
      case 'CAN_VIEW': return 'bg-slate-600/80 text-slate-100 border-slate-500';
      case 'CAN_RUN': return 'bg-blue-600/80 text-blue-100 border-blue-500';
      case 'CAN_EDIT': return 'bg-purple-600/80 text-purple-100 border-purple-500';
      case 'CAN_MANAGE': return 'bg-orange-600/80 text-orange-100 border-orange-500';
      default: return 'bg-slate-600/80 text-slate-100 border-slate-500';
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-purple-500 mb-4"></div>
          <p className="text-gray-400 text-lg">Loading administration panel...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8 max-w-7xl">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-100 mb-2">Administration</h1>
          <p className="text-gray-400">Manage user roles and application settings</p>
        </div>
        <button 
          onClick={loadData}
          className="px-4 py-2 bg-charcoal-500 text-gray-200 border border-charcoal-200 rounded-lg hover:bg-charcoal-400 transition-all duration-200 font-medium shadow-sm"
        >
          Refresh
        </button>
      </div>

      {/* Notification */}
      {notification && (
        <div className={`p-4 rounded-xl border-2 shadow-lg animate-fade-in ${
          notification.type === 'success' 
            ? 'bg-green-900/30 border-green-600 text-green-100' 
            : 'bg-red-900/30 border-red-600 text-red-100'
        }`}>
          <div className="flex items-center">
            <div className="flex-1 font-medium">{notification.message}</div>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="bg-red-900/30 border-2 border-red-600 rounded-xl p-5 text-red-100 shadow-lg">
          <div className="font-semibold mb-1">Error</div>
          <div className="text-red-200">{error}</div>
        </div>
      )}

      {/* User Role Management */}
      <div className="bg-charcoal-600 rounded-xl border border-charcoal-200 shadow-xl overflow-hidden">
        <div className="px-8 py-5 border-b border-charcoal-200 bg-gradient-to-r from-charcoal-600 to-charcoal-500">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-2xl font-bold text-gray-100">User Role Management</h2>
              <p className="text-gray-400 text-sm mt-1">Manage permissions for all users</p>
            </div>
            <input
              type="text"
              placeholder="Search users..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="px-4 py-2 bg-charcoal-400 border border-charcoal-200 rounded-lg text-gray-200 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-purple-500 w-64"
            />
          </div>
        </div>
        <div className="p-8">
          {filteredAndSortedUsers.length === 0 ? (
            <div className="text-center py-16">
              <div className="text-gray-500 text-lg mb-2">
                {searchQuery ? 'No users match your search' : 'No users found'}
              </div>
              <p className="text-gray-600 text-sm">
                {searchQuery ? 'Try a different search term' : 'Users will appear here when they access the application'}
              </p>
            </div>
          ) : (
            <div className="overflow-hidden rounded-lg border border-charcoal-200">
              <table className="w-full">
                <thead className="bg-charcoal-400 border-b border-charcoal-200">
                  <tr>
                    <th 
                      onClick={() => handleSort('user_email')}
                      className="text-left py-2 px-3 text-gray-300 font-semibold text-xs uppercase tracking-wider cursor-pointer hover:bg-charcoal-300 transition-colors"
                    >
                      <div className="flex items-center gap-2">
                        User Email
                        {sortConfig.key === 'user_email' && (
                          <span className="text-rust">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>
                        )}
                      </div>
                    </th>
                    <th 
                      onClick={() => handleSort('role')}
                      className="text-left py-2 px-3 text-gray-300 font-semibold text-xs uppercase tracking-wider cursor-pointer hover:bg-charcoal-300 transition-colors"
                    >
                      <div className="flex items-center gap-2">
                        Role
                        {sortConfig.key === 'role' && (
                          <span className="text-rust">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>
                        )}
                      </div>
                    </th>
                    <th className="text-right py-2 px-3 text-gray-300 font-semibold text-xs uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-charcoal-200">
                  {filteredAndSortedUsers.map(user => (
                    <tr key={user.user_email} className="hover:bg-charcoal-500/50 transition-colors duration-150">
                      <td className="py-2 px-3 text-gray-200 text-sm">{user.user_email}</td>
                      <td className="py-2 px-3">
                        <select
                          value={user.role}
                          onChange={(e) => updateUserRole(user.user_email, e.target.value)}
                          className={`px-2 py-1 rounded border cursor-pointer font-medium text-xs transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-purple-500 ${getRoleBadgeColor(user.role)}`}
                        >
                          <option value="CAN_VIEW">CAN_VIEW</option>
                          <option value="CAN_RUN">CAN_RUN</option>
                          <option value="CAN_EDIT">CAN_EDIT</option>
                          <option value="CAN_MANAGE">CAN_MANAGE</option>
                        </select>
                      </td>
                      <td className="py-2 px-3 text-right">
                        <button
                          onClick={() => deleteUser(user.user_email)}
                          className="px-2 py-1 text-xs bg-red-900/40 text-red-300 border border-red-700 rounded hover:bg-red-900/60 transition-all font-medium"
                          title="Remove user"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Application Settings */}
      <div className="bg-charcoal-600 rounded-xl border border-charcoal-200 shadow-xl overflow-hidden">
        <div className="px-8 py-5 border-b border-charcoal-200 bg-gradient-to-r from-charcoal-600 to-charcoal-500">
          <h2 className="text-2xl font-bold text-gray-100">Application Settings</h2>
          <p className="text-gray-400 text-sm mt-1">Configure default behavior for new users</p>
        </div>
        <div className="p-6">
          <label className="block text-gray-300 font-medium mb-2 text-xs uppercase tracking-wider">Default User Role</label>
          <div className="flex items-center gap-3">
            <select
              value={config.default_user_role || 'CAN_MANAGE'}
              onChange={(e) => setConfig({ ...config, default_user_role: e.target.value })}
              className="px-3 py-2 bg-charcoal-400 text-gray-100 border border-charcoal-200 rounded-md focus:outline-none focus:ring-2 focus:ring-purple-500 text-sm transition-all"
            >
              <option value="CAN_VIEW">CAN_VIEW - Read Only</option>
              <option value="CAN_RUN">CAN_RUN - Default User</option>
              <option value="CAN_EDIT">CAN_EDIT - Power User</option>
              <option value="CAN_MANAGE">CAN_MANAGE - Administrator</option>
            </select>
            <button
              onClick={saveDefaultRole}
              disabled={saving}
              className="px-4 py-2 bg-purple-600 text-white rounded-md hover:bg-purple-500 disabled:opacity-50 disabled:cursor-not-allowed font-medium text-sm transition-all shadow-sm"
            >
              {saving ? (
                <span className="flex items-center gap-2">
                  <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-white"></div>
                  Saving...
                </span>
              ) : (
                'Save'
              )}
            </button>
          </div>
          <p className="text-gray-500 text-xs mt-2">
            Role assigned to new users on first access
          </p>
        </div>
      </div>

      {/* Role Definitions */}
      <div className="bg-charcoal-600 rounded-xl border border-charcoal-200 shadow-xl overflow-hidden">
        <div className="px-8 py-5 border-b border-charcoal-200 bg-gradient-to-r from-charcoal-600 to-charcoal-500">
          <h2 className="text-2xl font-bold text-gray-100">Role Definitions</h2>
          <p className="text-gray-400 text-sm mt-1">Permission levels and their capabilities</p>
        </div>
        <div className="p-8">
          <div className="space-y-3 text-gray-300 text-sm">
            <div className="flex items-start gap-3">
              <span className={`px-2 py-1 rounded text-xs font-bold border ${getRoleBadgeColor('CAN_VIEW')}`}>CAN_VIEW</span>
              <span className="flex-1">Read-only access to all data</span>
            </div>
            <div className="flex items-start gap-3">
              <span className={`px-2 py-1 rounded text-xs font-bold border ${getRoleBadgeColor('CAN_RUN')}`}>CAN_RUN</span>
              <span className="flex-1">Can trigger validations, create/edit own tables, queries, and schedules</span>
            </div>
            <div className="flex items-start gap-3">
              <span className={`px-2 py-1 rounded text-xs font-bold border ${getRoleBadgeColor('CAN_EDIT')}`}>CAN_EDIT</span>
              <span className="flex-1">Can edit any table, query, or schedule</span>
            </div>
            <div className="flex items-start gap-3">
              <span className={`px-2 py-1 rounded text-xs font-bold border ${getRoleBadgeColor('CAN_MANAGE')}`}>CAN_MANAGE</span>
              <span className="flex-1">Full admin access including systems and type mappings</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

