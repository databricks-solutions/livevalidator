import React, { useState, useEffect } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { Checkbox } from '../components/Checkbox';
import { apiCall } from '../services/api';

export function ConfigurationView() {
  const [config, setConfig] = useState({
    downgrade_unicode: false,
    replace_special_char: [],
    extra_replace_regex: ''
  });
  const [originalConfig, setOriginalConfig] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [successMessage, setSuccessMessage] = useState('');

  const hasChanges = originalConfig && JSON.stringify(config) !== JSON.stringify(originalConfig);

  useEffect(() => {
    loadConfig();
  }, []);

  const loadConfig = async () => {
    try {
      setLoading(true);
      const data = await apiCall('GET', '/api/validation-config');
      setConfig(data);
      setOriginalConfig(data);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      setError(null);
      const updated = await apiCall('PUT', '/api/validation-config', config);
      setConfig(updated);
      setOriginalConfig(updated);
      setSuccessMessage('Configuration saved successfully');
      setTimeout(() => setSuccessMessage(''), 3000);
    } catch (err) {
      setError(err);
    } finally {
      setSaving(false);
    }
  };

  const handleCheckboxChange = (checked) => {
    setConfig(prev => ({ ...prev, downgrade_unicode: checked }));
  };

  const handleMaxHexChange = (value) => {
    const current = config.replace_special_char || [];
    const newArr = [value.trim().toUpperCase(), current[1] || '?'];
    setConfig(prev => ({ ...prev, replace_special_char: newArr }));
  };

  const handleReplacementCharChange = (value) => {
    const current = config.replace_special_char || [];
    const newArr = [current[0] || '7F', value];
    setConfig(prev => ({ ...prev, replace_special_char: newArr }));
  };

  const handleRegexChange = (value) => {
    setConfig(prev => ({ ...prev, extra_replace_regex: value }));
  };

  // Detect potentially dangerous unescaped regex characters
  const hasUnescapedSpecialChars = (pattern) => {
    if (!pattern) return false;
    // Check for unescaped dots (periods) - most common mistake
    // Find all dots and check if they're preceded by backslash
    for (let i = 0; i < pattern.length; i++) {
      if (pattern[i] === '.') {
        // Check if this dot is escaped
        let backslashCount = 0;
        for (let j = i - 1; j >= 0 && pattern[j] === '\\'; j--) {
          backslashCount++;
        }
        // If even number of backslashes (including 0), the dot is unescaped
        if (backslashCount % 2 === 0) {
          return true;
        }
      }
    }
    return false;
  };

  const regexWarning = config.extra_replace_regex && hasUnescapedSpecialChars(config.extra_replace_regex);

  if (loading) {
    return <div className="text-gray-400">Loading configuration...</div>;
  }

  return (
    <>
      {error && <ErrorBox message={error.message} onClose={() => setError(null)} />}
      {successMessage && (
        <div className="mb-4 p-3 bg-green-900/40 border border-green-700 rounded-lg text-green-300">
          {successMessage}
        </div>
      )}

      <div className="mb-4 flex items-start justify-between">
        <div>
          <h2 className="text-3xl font-bold text-rust-light mb-1 flex items-center gap-2">
            <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            Global Configs
          </h2>
          <p className="text-gray-400 text-base">
            These settings apply to all validations unless overridden at the table/query level.
          </p>
        </div>
        {hasChanges && (
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-6 py-2 bg-rust-light text-white rounded-lg hover:bg-rust-dark transition-all disabled:opacity-50 font-medium"
          >
            {saving ? 'Saving...' : 'Save Configuration'}
          </button>
        )}
      </div>

      <div className="bg-charcoal-500 border border-charcoal-200 rounded-lg overflow-hidden">
        <table className="w-full">
          <thead className="bg-charcoal-400 border-b border-charcoal-200">
            <tr>
              <th className="text-left px-4 py-3 text-sm text-gray-300 font-semibold w-1/4">Setting</th>
              <th className="text-left px-4 py-3 text-sm text-gray-300 font-semibold w-1/2">Value / Description</th>
              <th className="text-center px-4 py-3 text-sm text-gray-300 font-semibold w-1/4">Enabled</th>
            </tr>
          </thead>
          <tbody>
            {/* Downgrade Unicode */}
            <tr className="border-b border-charcoal-300/30">
              <td className="px-4 py-4 text-gray-200 font-medium align-top">
                Downgrade Unicode Characters
              </td>
              <td className="px-4 py-4 text-gray-400 text-sm">
                <div className="space-y-2">
                  <p className="font-medium">Applies 4 transformations to normalize Unicode text:</p>
                  <div className="pl-3 space-y-1.5 text-xs">
                    <div>
                      <span className="font-semibold text-gray-300">1. Spaces:</span>
                      <span className="ml-2">Non-breaking spaces → regular spaces</span>
                    </div>
                    <div>
                      <span className="font-semibold text-gray-300">2. Quotes:</span>
                      <span className="ml-2">'→` (left quote to backtick)</span>
                    </div>
                    <div>
                      <span className="font-semibold text-gray-300">3. Diacritics:</span>
                      <span className="ml-2">é→e, ñ→n, ü→u, ç→c, à→a, ö→o</span>
                    </div>
                    <div>
                      <span className="font-semibold text-gray-300">4. Symbols:</span>
                      <span className="ml-2">™→TM, ©→(c), ®→(R), æ→ae, œ→oe, ℃→°C</span>
                    </div>
                  </div>
                  <p className="text-xs text-gray-500 italic pt-1">Uses Unicode NFKD normalization + character replacement based on hex range below</p>
                </div>
              </td>
              <td className="px-4 py-4 text-center align-top">
                <Checkbox
                  checked={config.downgrade_unicode}
                  onChange={(e) => handleCheckboxChange(e.target.checked)}
                />
              </td>
            </tr>

            {/* Replace Special Characters */}
            <tr className="border-b border-charcoal-300/30">
              <td className="px-4 py-4 text-gray-200 font-medium align-top">
                Replace Special Characters
                {!config.downgrade_unicode && (
                  <div className="text-xs text-gray-500 mt-1 font-normal">
                    (only used if downgrade enabled)
                  </div>
                )}
              </td>
              <td className="px-4 py-4">
                <div className="space-y-3">
                  {/* Inputs in one line */}
                  <div className="flex items-start gap-4">
                    {/* Max Hex Code */}
                    <div>
                      <label className="block text-sm text-gray-300 mb-1.5 font-medium">Max Hex Code</label>
                      <input
                        type="text"
                        value={config.replace_special_char[0] || ''}
                        onChange={(e) => handleMaxHexChange(e.target.value)}
                        disabled={!config.downgrade_unicode}
                        placeholder="7F"
                        maxLength={2}
                        className="w-20 px-3 py-2 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm font-mono text-center focus:outline-none focus:border-rust-light disabled:opacity-50 disabled:cursor-not-allowed"
                      />
                      <div className="mt-1 text-xs text-gray-500">Upper bound</div>
                    </div>

                    {/* Replacement Character */}
                    <div>
                      <label className="block text-sm text-gray-300 mb-1.5 font-medium">Replacement Char</label>
                      <input
                        type="text"
                        value={config.replace_special_char[1] || ''}
                        onChange={(e) => handleReplacementCharChange(e.target.value)}
                        disabled={!config.downgrade_unicode}
                        placeholder="?"
                        maxLength={1}
                        className="w-20 px-3 py-2 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm font-mono text-center focus:outline-none focus:border-rust-light disabled:opacity-50 disabled:cursor-not-allowed"
                      />
                      <div className="mt-1 text-xs text-gray-500">Substitution</div>
                    </div>

                    {/* Generated Command */}
                    <div className="flex-1">
                      <label className="block text-sm text-gray-300 mb-1.5 font-medium">Generated Regex Pattern</label>
                      <div className="px-3 py-2 bg-charcoal-600 border border-charcoal-300 rounded text-gray-200 text-sm font-mono">
                        <code className="text-xs text-purple-300">
                          regexp_replace(col, "[^\x00-\x{config.replace_special_char[0] || '7F'}]", "{config.replace_special_char[1] || '?'}")
                        </code>
                      </div>
                      <div className="mt-1 text-xs text-gray-500">Characters outside range will be replaced</div>
                    </div>
                  </div>
                </div>
              </td>
              <td className="px-4 py-4 text-center align-top">
                <span className="text-gray-500">-</span>
              </td>
            </tr>

            {/* Extra Replace Regex */}
            <tr>
              <td className="px-4 py-4 text-gray-200 font-medium align-top">
                Extra Replace Regex
                {!config.downgrade_unicode && (
                  <div className="text-xs text-gray-500 mt-1 font-normal">
                    (only used if downgrade enabled)
                  </div>
                )}
              </td>
              <td className="px-4 py-4">
                <input
                  type="text"
                  value={config.extra_replace_regex}
                  onChange={(e) => handleRegexChange(e.target.value)}
                  disabled={!config.downgrade_unicode}
                  placeholder="\.\.\."
                  className={`w-full px-3 py-2 bg-charcoal-600 rounded text-gray-200 text-sm focus:outline-none disabled:opacity-50 disabled:cursor-not-allowed font-mono ${
                    regexWarning 
                      ? 'border-2 border-yellow-500 focus:border-yellow-500' 
                      : 'border border-charcoal-300 focus:border-rust-light'
                  }`}
                />
                {regexWarning && (
                  <div className="mt-2 p-2 bg-yellow-900/30 border border-yellow-600 rounded text-yellow-300 text-xs">
                    <div className="font-semibold mb-1">⚠️ Warning: Unescaped special characters detected!</div>
                    <div className="text-yellow-200/90">
                      The <code className="px-1 bg-yellow-900/40 rounded">.</code> character matches ANY character. 
                      To match a literal period, use <code className="px-1 bg-yellow-900/40 rounded">\.</code> instead.
                      <div className="mt-1">Example: Use <code className="px-1 bg-yellow-900/40 rounded">\.\.\.</code> to match three dots.</div>
                    </div>
                  </div>
                )}
                <div className="mt-2 text-xs text-gray-500">
                  <p className="font-medium text-gray-400 mb-1">Raw regex pattern (matched text replaced with replacement char)</p>
                  <p>⚠️ Special regex characters must be escaped: <code className="px-1 py-0.5 bg-charcoal-700 rounded font-mono">\.</code> for dot, <code className="px-1 py-0.5 bg-charcoal-700 rounded font-mono">\*</code> for asterisk, etc.</p>
                </div>
              </td>
              <td className="px-4 py-4 text-center align-top">
                <span className="text-gray-500">-</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </>
  );
}

