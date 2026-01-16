import React, { useState } from 'react';
import { apiCall } from '../services/api';

export function SetupView() {
  const [initLoading, setInitLoading] = useState(false);
  const [resetLoading, setResetLoading] = useState(false);

  const handleInitialize = async () => {
    setInitLoading(true);
    try {
      await apiCall('POST', '/api/setup/initialize-database');
      alert('Database initialized successfully!');
    } catch (err) {
      alert(`Initialization failed: ${err.message}`);
    } finally {
      setInitLoading(false);
    }
  };

  const handleReset = async () => {
    if (!confirm('⚠️ WARNING: This will DELETE ALL DATA and recreate tables. Are you absolutely sure?')) return;
    if (!confirm('This action CANNOT be undone. Type YES in your mind and click OK to proceed.')) return;
    
    setResetLoading(true);
    try {
      await apiCall('POST', '/api/setup/reset-database');
      alert('Database reset successfully!');
    } catch (err) {
      alert(`Reset failed: ${err.message}`);
    } finally {
      setResetLoading(false);
    }
  };

  return (
    <>
      <h2 className="text-2xl font-semibold text-rust-light mb-4">Database Setup</h2>
      
      <div className="max-w-2xl">
        {/* Initial Setup Section */}
        <div className="bg-purple-900/30 border-2 border-purple-600 rounded-lg p-6 mb-6">
          <h3 className="text-purple-400 font-bold text-xl mb-4">🚀 Initial Setup</h3>
          <p className="text-gray-300 mb-6">
            Follow these steps to set up a <strong className="text-purple-400">fresh deployment</strong>. 
            Each step only needs to be completed once per database instance.
          </p>

          {/* Step 1 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">1</span>
              Run SQL Commands as Database Creator
            </h4>
            <p className="text-gray-300 text-sm mb-3 ml-9">
              The <strong>creator</strong> of the Lakebase PostgreSQL instance must run the following commands in the <strong>Databricks SQL Editor</strong>:
            </p>
            <pre className="bg-charcoal-800 p-3 rounded text-xs text-gray-200 overflow-x-auto border border-gray-700 ml-9">
{`CREATE USER apprunner WITH PASSWORD 'beepboop123';

CREATE SCHEMA IF NOT EXISTS control;
GRANT USAGE ON SCHEMA control to apprunner;
GRANT apprunner TO CURRENT_USER;
ALTER SCHEMA control OWNER TO apprunner;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO apprunner;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO apprunner;

ALTER DEFAULT PRIVILEGES IN SCHEMA control 
GRANT USAGE, SELECT ON SEQUENCES TO apprunner;`}
            </pre>
          </div>

          {/* Step 2 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">2</span>
              Initialize the Database
            </h4>
            <p className="text-gray-300 text-sm mb-3 ml-9">
              Click the button below to create all necessary tables and indexes:
            </p>
            <div className="ml-9">
              <button
                onClick={handleInitialize}
                disabled={initLoading}
                className="px-4 py-2 bg-purple-600 text-gray-100 rounded-md hover:bg-purple-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {initLoading ? 'Initializing...' : '✨ Initialize Database'}
              </button>
            </div>
          </div>

          {/* Step 3 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">3</span>
              Hard refresh
            </h4>
            <p className="text-gray-300 text-sm ml-9">
              Hard-refresh the app using Cmd + Shift + R (Mac) or Ctrl + Shift + R (Windows). Now navigate back to the Setup tab on bottom right.
            </p>
          </div>

          {/* Step 4 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">4</span>
              Configure Source and Target Systems
            </h4>
            <p className="text-gray-300 text-sm ml-9">
              Go to the <strong>Systems</strong> tab and add your source and target database systems (e.g., Databricks catalogs, Netezza, Teradata, etc.).
            </p>
          </div>

          {/* Step 5 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">5</span>
              Define Secrets
            </h4>
            <p className="text-gray-300 text-sm mb-3 ml-9">
              While eng is working on an oauth token exchange, add secrets for the LiveValidator app service principal.
              Get the values from: <a 
                href="/api/secrets" 
                target="_blank" 
                rel="noopener noreferrer"
                className="text-blue-400 hover:text-blue-300 underline"
              >/api/secrets</a>
            </p>
            <pre className="bg-charcoal-800 p-3 rounded text-xs text-gray-200 overflow-x-auto border border-gray-700 ml-9">
{`from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

w = WorkspaceClient()
w.secrets.create_scope('livevalidator')
w.secrets.put_acl('livevalidator', 'users', AclPermission.READ)
w.secrets.put_secret('livevalidator', key="lv-app-id", string_value=<first value>)
w.secrets.put_secret('livevalidator', key="lv-app-secret", string_value=<second value>)`}
            </pre>
            <div>
            <p className="text-gray-300 text-sm mb-3 ml-9">
              Add secrets for any JDBC authentication if needed:
            </p>
            <pre className="bg-charcoal-800 p-3 rounded text-xs text-gray-200 overflow-x-auto border border-gray-700 ml-9">
{`w.secrets.put_secret('livevalidator', key="mysystem_user", string_value="my_user")
w.secrets.put_secret('livevalidator', key="mysystem_pass", string_value="*******")`}
            </pre>
            </div>
          </div>

          {/* Step 6 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">6</span>
              Start <strong>Job Sentinel</strong>
            </h4>
            <p className="text-gray-300 text-sm ml-9">
              Run the command to begin the daemon which handles triggers and schedules
            </p>
            <pre className="bg-charcoal-800 p-3 rounded text-xs text-gray-200 overflow-x-auto border border-gray-700 ml-9">
{`databricks bundle run job_sentinel --no-wait -t <your target>`}
            </pre>
            <p className="text-gray-300 text-sm ml-9">
            You can also trigger it from the Jobs UI
            </p>
          </div>

          {/* Step 7 */}
          <div className="mb-5">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">7</span>
              Add Tables and Queries
            </h4>
            <p className="text-gray-300 text-sm ml-9">
              Go to the <strong>Tables</strong> and <strong>Queries</strong> tabs to define what you want to validate. 
              Bind them to schedules (see next step) or run them manually with the ▶️ button.
            </p>
          </div>

          {/* Step 8 */}
          <div className="mb-5 pb-5 border-b border-gray-700">
            <h4 className="text-purple-300 font-semibold text-lg mb-3">
              <span className="bg-purple-600 text-white rounded-full w-7 h-7 inline-flex items-center justify-center mr-2 text-sm">8</span>
              Create Schedules
            </h4>
            <p className="text-gray-300 text-sm ml-9">
              Go to the <strong>Schedules</strong> tab and define when your validations should run (e.g., daily, weekly).
            </p>
          </div>
        </div>

        {/* Danger Zone Section */}
        <div className="bg-red-900/20 border-2 border-red-600 rounded-lg p-6">
          <h3 className="text-red-400 font-bold text-xl mb-4">⚠️ Danger Zone</h3>
          <p className="text-gray-300 mb-4">
            Use this to completely <strong className="text-red-400">reset the database</strong>. 
            This will <strong>delete all tables and data</strong>, then recreate the schema.
          </p>
          <p className="text-red-300 text-sm mb-4">
            ⚠️ <strong>WARNING:</strong> This action cannot be undone and will delete all your validation history, schedules, and configurations.
          </p>
          <button
            onClick={handleReset}
            disabled={resetLoading}
            className="px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {resetLoading ? 'Resetting...' : '🗑️ Reset Database'}
          </button>
        </div>
      </div>
    </>
  );
}
