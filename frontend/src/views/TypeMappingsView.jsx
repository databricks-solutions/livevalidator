import React, { useState, useEffect } from 'react';
import { ErrorBox } from '../components/ErrorBox';
import { systemService, typeTransformationService } from '../services/api';

export function TypeMappingsView() {
  const [systems, setSystems] = useState([]);
  const [transformations, setTransformations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState({});
  const [validating, setValidating] = useState({});
  const [validationErrors, setValidationErrors] = useState({});
  const [error, setError] = useState(null);
  const [successMessage, setSuccessMessage] = useState('');
  const [editedFunctions, setEditedFunctions] = useState({});
  const [defaultFunctions, setDefaultFunctions] = useState({});
  const [collapsedCards, setCollapsedCards] = useState({});
  const [initialCollapseSet, setInitialCollapseSet] = useState(false);

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      const [systemsData, transformationsData] = await Promise.all([
        systemService.list(),
        typeTransformationService.list()
      ]);
      console.log('Loaded systems:', systemsData);
      console.log('Loaded transformations:', transformationsData);
      setSystems(systemsData);
      setTransformations(transformationsData);
      
      // Pre-load default functions for all system kinds
      const uniqueKinds = [...new Set(systemsData.map(s => s.kind))];
      console.log('Unique system kinds:', uniqueKinds);
      const defaults = {};
      await Promise.all(
        uniqueKinds.map(async (kind) => {
          try {
            const result = await typeTransformationService.getDefault(kind);
            console.log(`Loaded default for ${kind}:`, result.function.substring(0, 50) + '...');
            defaults[kind] = result.function;
          } catch (err) {
            console.error(`Failed to load default for ${kind}:`, err);
          }
        })
      );
      console.log('All defaults loaded:', Object.keys(defaults));
      setDefaultFunctions(defaults);
    } catch (err) {
      console.error('Error loading data:', err);
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  // Generate all possible system pairs
  const generateSystemPairs = () => {
    const pairs = [];
    const activeSystems = systems.filter(s => s.is_active);
    const toCollapse = {};
    
    for (let i = 0; i < activeSystems.length; i++) {
      for (let j = i + 1; j < activeSystems.length; j++) {
        const sysA = activeSystems[i];
        const sysB = activeSystems[j];
        
        // Find existing transformation for this pair
        const existing = transformations.find(t => 
          (t.system_a_id === sysA.id && t.system_b_id === sysB.id) ||
          (t.system_a_id === sysB.id && t.system_b_id === sysA.id)
        );
        
        const pairKey = `${Math.min(sysA.id, sysB.id)}_${Math.max(sysA.id, sysB.id)}`;
        
        pairs.push({
          systemA: sysA,
          systemB: sysB,
          transformation: existing,
          pairKey,
          hasTransformation: existing && (existing.system_a_function || existing.system_b_function)
        });
        
        // Mark for collapse if no transformation exists
        if (!existing || (!existing.system_a_function && !existing.system_b_function)) {
          toCollapse[pairKey] = true;
        }
      }
    }
    
    // Set initial collapse state once
    if (!initialCollapseSet && Object.keys(toCollapse).length > 0) {
      setCollapsedCards(toCollapse);
      setInitialCollapseSet(true);
    }
    
    // Sort: pairs with transformations first, then alphabetically
    return pairs.sort((a, b) => {
      if (a.hasTransformation && !b.hasTransformation) return -1;
      if (!a.hasTransformation && b.hasTransformation) return 1;
      return a.systemA.name.localeCompare(b.systemA.name);
    });
  };


  const validateCode = async (pairKey, field, code) => {
    const validationKey = `${pairKey}_${field}`;
    setValidating(prev => ({ ...prev, [validationKey]: true }));
    
    try {
      const result = await typeTransformationService.validateCode(code);
      
      if (!result.valid) {
        setValidationErrors(prev => ({
          ...prev,
          [validationKey]: result.errors
        }));
      } else {
        setValidationErrors(prev => {
          const newErrors = { ...prev };
          delete newErrors[validationKey];
          return newErrors;
        });
      }
      
      return result.valid;
    } catch (err) {
      setValidationErrors(prev => ({
        ...prev,
        [validationKey]: [{ type: 'error', message: err.message, line: 1 }]
      }));
      return false;
    } finally {
      setValidating(prev => ({ ...prev, [validationKey]: false }));
    }
  };

  const handleSave = async (pair) => {
    const { systemA, systemB, transformation, pairKey } = pair;
    
    const systemAFunc = editedFunctions[`${pairKey}_system_a`];
    const systemBFunc = editedFunctions[`${pairKey}_system_b`];
    
    // Trim functions - if only whitespace, treat as empty
    const trimmedA = systemAFunc !== undefined ? systemAFunc.trim() : undefined;
    const trimmedB = systemBFunc !== undefined ? systemBFunc.trim() : undefined;
    
    // Only save if user has actually edited the functions (not using defaults)
    if (!trimmedA && !trimmedB && !transformation) {
      setError(new Error('No changes to save. Edit the functions to save custom transformations.'));
      return;
    }
    
    // Validate both functions (only if non-empty after trim)
    setSaving(prev => ({ ...prev, [pairKey]: true }));
    
    const [validA, validB] = await Promise.all([
      trimmedA ? validateCode(pairKey, 'system_a', trimmedA) : true,
      trimmedB ? validateCode(pairKey, 'system_b', trimmedB) : true
    ]);
    
    if (!validA || !validB) {
      setSaving(prev => ({ ...prev, [pairKey]: false }));
      setError(new Error('Please fix validation errors before saving'));
      return;
    }
    
    try {
      if (transformation) {
        // Update existing - need to map functions correctly if IDs are swapped
        const isSwapped = systemA.id !== transformation.system_a_id;
        
        // Get current functions, accounting for swap
        const currentFuncA = isSwapped ? transformation.system_b_function : transformation.system_a_function;
        const currentFuncB = isSwapped ? transformation.system_a_function : transformation.system_b_function;
        
        await typeTransformationService.update(
          systemA.id,
          systemB.id,
          {
            system_a_function: trimmedA !== undefined ? trimmedA : currentFuncA,
            system_b_function: trimmedB !== undefined ? trimmedB : currentFuncB,
            version: transformation.version
          }
        );
      } else {
        // Create new - send functions matched to their system IDs
        // The backend will normalize storage order, but we send based on which system the function is for
        const funcA = trimmedA || '';
        const funcB = trimmedB || '';
        
        const payload = {
          system_a_id: systemA.id,
          system_b_id: systemB.id,
          system_a_function: funcA,  // Function for systemA (whatever ID that is)
          system_b_function: funcB   // Function for systemB (whatever ID that is)
        };
        
        console.log('[FRONTEND CREATE] Sending to backend:', {
          'UI Left (systemA)': `${systemA.name} (ID ${systemA.id})`,
          'UI Right (systemB)': `${systemB.name} (ID ${systemB.id})`,
          'payload': payload,
          'funcA preview': funcA.substring(0, 50),
          'funcB preview': funcB.substring(0, 50)
        });
        
        await typeTransformationService.create(payload);
      }
      
      // Clear edited state for this pair
      setEditedFunctions(prev => {
        const newState = { ...prev };
        delete newState[`${pairKey}_system_a`];
        delete newState[`${pairKey}_system_b`];
        return newState;
      });
      
      setSuccessMessage(`Saved transformations for ${systemA.name} ↔ ${systemB.name}`);
      setTimeout(() => setSuccessMessage(''), 3000);
      
      // Reload data
      await loadData();
    } catch (err) {
      setError(err);
    } finally {
      setSaving(prev => ({ ...prev, [pairKey]: false }));
    }
  };

  const handleFunctionChange = (pairKey, field, value) => {
    setEditedFunctions(prev => ({
      ...prev,
      [`${pairKey}_${field}`]: value
    }));
    
    // Clear validation errors when user edits
    const validationKey = `${pairKey}_${field}`;
    setValidationErrors(prev => {
      const newErrors = { ...prev };
      delete newErrors[validationKey];
      return newErrors;
    });
  };

  const getDisplayFunction = (pair, field) => {
    const editedKey = `${pair.pairKey}_${field}`;
    if (editedFunctions[editedKey] !== undefined) {
      return editedFunctions[editedKey];
    }
    
    if (pair.transformation) {
      // Check if stored order matches display order
      const isSwapped = pair.systemA.id !== pair.transformation.system_a_id;
      
      // Map to correct function based on whether IDs are swapped
      let savedFunc;
      if (field === 'system_a') {
        savedFunc = isSwapped ? pair.transformation.system_b_function : pair.transformation.system_a_function;
      } else {
        savedFunc = isSwapped ? pair.transformation.system_a_function : pair.transformation.system_b_function;
      }
      
      // If saved function exists and is not empty, use it
      // Otherwise show placeholder for undefined
      if (savedFunc && savedFunc.trim()) {
        return savedFunc;
      } else {
        return '# no type transformation defined';
      }
    }
    
    // Return default function as the value (not placeholder)
    const system = field === 'system_a' ? pair.systemA : pair.systemB;
    return defaultFunctions[system.kind] || '';
  };
  
  const isUsingDefault = (pair, field) => {
    const editedKey = `${pair.pairKey}_${field}`;
    
    // If user has edited it, it's not using default
    if (editedFunctions[editedKey] !== undefined) {
      return false;
    }
    
    // If no transformation exists, use default
    if (!pair.transformation) {
      return true;
    }
    
    // If transformation exists but this side is empty, show as grey placeholder
    // Check if stored order matches display order
    const isSwapped = pair.systemA.id !== pair.transformation.system_a_id;
    
    let savedFunc;
    if (field === 'system_a') {
      savedFunc = isSwapped ? pair.transformation.system_b_function : pair.transformation.system_a_function;
    } else {
      savedFunc = isSwapped ? pair.transformation.system_a_function : pair.transformation.system_b_function;
    }
    
    return !savedFunc || !savedFunc.trim();
  };

  const hasChanges = (pair) => {
    return editedFunctions[`${pair.pairKey}_system_a`] !== undefined ||
           editedFunctions[`${pair.pairKey}_system_b`] !== undefined;
  };

  const toggleCard = (pairKey) => {
    setCollapsedCards(prev => ({
      ...prev,
      [pairKey]: !prev[pairKey]
    }));
  };

  if (loading) {
    return <div className="text-gray-400">Loading type mappings...</div>;
  }

  const pairs = generateSystemPairs();

  if (pairs.length === 0) {
    return (
      <div>
        <h2 className="text-3xl font-bold text-rust-light mb-4 flex items-center gap-2">
          <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          Type Mappings
        </h2>
        <p className="text-gray-400">
          You need at least 2 active systems to configure type mappings. 
          Go to the Systems tab to add systems.
        </p>
      </div>
    );
  }

  return (
    <>
      {error && <ErrorBox message={error.message} onClose={() => setError(null)} />}
      {successMessage && (
        <div className="mb-4 p-3 bg-green-900/40 border border-green-700 rounded-lg text-green-300">
          {successMessage}
        </div>
      )}

      <div className="mb-6">
        <h2 className="text-3xl font-bold text-rust-light mb-2 flex items-center gap-2">
          <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          Type Mappings
        </h2>
        <p className="text-gray-400 text-base">
          Define how column types are transformed when comparing data between different systems.
          Each system pair gets its own transformation functions.
        </p>
      </div>

      {/* Instructions */}
      <div className="mb-6 bg-charcoal-500 border border-charcoal-200 rounded-lg p-4">
        <h3 className="text-sm font-semibold text-gray-300 mb-3">Instructions</h3>
        
        {/* Description - Full Width */}
        <p className="text-xs text-gray-400 mb-2">
          Define transform_columns() for each system in a system pair Returns a SQL expression string for each column. Use this to cast types, trim strings, or pass through columns unchanged. Validation jobs for each system pair will be given the functions defined below.
        </p>
        
        {/* Examples Note */}
        <h3 className="text-sm font-semibold text-gray-300 mb-3">Examples</h3>
        <p className="text-xs text-gray-400 mb-2">
          Retain decimal/numeric types for precision while casting all other types to string for comparison. Examples:
        </p>
        
        {/* Code Examples - Full Width */}
        <div className="grid grid-cols-3 gap-3">
          {/* Databricks */}
          <div className="bg-charcoal-600 rounded p-3 border border-charcoal-300">
            <div className="font-semibold text-gray-300 mb-2 text-sm">Databricks</div>
            <pre className="font-mono text-[10px] text-gray-400 leading-tight">{`def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case other if other.startswith('Decimal'):
            return column_name
        case _:
            return f"CAST(\{column_name\} AS STRING)"`}</pre>
          </div>
          
          {/* Teradata */}
          <div className="bg-charcoal-600 rounded p-3 border border-charcoal-300">
            <div className="font-semibold text-gray-300 mb-2 text-sm">Teradata</div>
            <pre className="font-mono text-[10px] text-gray-400 leading-tight">{`def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case other if other.startswith('DECIMAL'):
            return column_name
        case _:
            return f"CAST(\{column_name\} AS VARCHAR(250))"`}</pre>
          </div>
          
          {/* Oracle */}
          <div className="bg-charcoal-600 rounded p-3 border border-charcoal-300">
            <div className="font-semibold text-gray-300 mb-2 text-sm">Oracle</div>
            <pre className="font-mono text-[10px] text-gray-400 leading-tight">{`def transform_columns(column_name: str, data_type: str) -> str:
    match data_type:
        case other if 'NUMBER' in other:
            return column_name
        case _:
            return f"CAST(\{column_name\} AS VARCHAR2(250))"`}</pre>
          </div>
        </div>
      </div>

      <div className="space-y-6">
        {pairs.map(pair => (
          <div key={pair.pairKey} className="bg-charcoal-500 border border-charcoal-200 rounded-lg p-6">
            {/* Header */}
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <button
                  onClick={() => toggleCard(pair.pairKey)}
                  className="text-gray-400 hover:text-rust-light transition-colors"
                  title={collapsedCards[pair.pairKey] ? "Expand" : "Collapse"}
                >
                  <svg className="w-5 h-5 transition-transform" style={{ transform: collapsedCards[pair.pairKey] ? 'rotate(-90deg)' : 'rotate(0deg)' }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                <div className="flex items-center gap-2">
                  <div className="text-xl font-semibold text-rust-light">{pair.systemA.name}</div>
                  <span className="px-2 py-1 bg-charcoal-600 rounded text-gray-400 text-xs">{pair.systemA.kind}</span>
                </div>
                <div className="text-gray-500 text-xl">↔</div>
                <div className="flex items-center gap-2">
                  <div className="text-xl font-semibold text-rust-light">{pair.systemB.name}</div>
                  <span className="px-2 py-1 bg-charcoal-600 rounded text-gray-400 text-xs">{pair.systemB.kind}</span>
                </div>
              </div>
              
              {hasChanges(pair) && !collapsedCards[pair.pairKey] && (
                <button
                  onClick={() => handleSave(pair)}
                  disabled={saving[pair.pairKey]}
                  className="px-4 py-2 bg-rust-light text-white rounded-lg hover:bg-rust-dark transition-all disabled:opacity-50 font-medium"
                >
                  {saving[pair.pairKey] ? 'Saving...' : 'Save Changes'}
                </button>
              )}
            </div>

            {/* Two column layout for functions - only show when not collapsed */}
            {!collapsedCards[pair.pairKey] && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {/* System A Function */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-semibold text-gray-300">
                    {pair.systemA.name}
                  </label>
                  {isUsingDefault(pair, 'system_a') && (
                    <span className="text-xs text-gray-500 italic">Default template</span>
                  )}
                </div>
                
                <textarea
                  value={getDisplayFunction(pair, 'system_a')}
                  onChange={(e) => handleFunctionChange(pair.pairKey, 'system_a', e.target.value)}
                  placeholder={`# Loading default for ${pair.systemA.kind}...`}
                  className={`w-full h-64 px-3 py-2 bg-charcoal-600 border border-charcoal-300 rounded text-xs font-mono focus:outline-none focus:border-rust-light resize-none ${
                    isUsingDefault(pair, 'system_a') ? 'text-gray-500' : 'text-gray-200'
                  }`}
                  spellCheck={false}
                />
                
                {validating[`${pair.pairKey}_system_a`] && (
                  <div className="mt-2 text-xs text-yellow-400">Validating...</div>
                )}
                
                {validationErrors[`${pair.pairKey}_system_a`] && (
                  <div className="mt-2 p-2 bg-red-900/30 border border-red-700 rounded text-xs">
                    {validationErrors[`${pair.pairKey}_system_a`].map((err, idx) => (
                      <div key={idx} className="text-red-300 mb-1">
                        <span className="font-semibold">[{err.type}]</span> Line {err.line}: {err.message}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* System B Function */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-semibold text-gray-300">
                    {pair.systemB.name}
                  </label>
                  {isUsingDefault(pair, 'system_b') && (
                    <span className="text-xs text-gray-500 italic">Default template</span>
                  )}
                </div>
                
                <textarea
                  value={getDisplayFunction(pair, 'system_b')}
                  onChange={(e) => handleFunctionChange(pair.pairKey, 'system_b', e.target.value)}
                  placeholder={`# Loading default for ${pair.systemB.kind}...`}
                  className={`w-full h-64 px-3 py-2 bg-charcoal-600 border border-charcoal-300 rounded text-xs font-mono focus:outline-none focus:border-rust-light resize-none ${
                    isUsingDefault(pair, 'system_b') ? 'text-gray-500' : 'text-gray-200'
                  }`}
                  spellCheck={false}
                />
                
                {validating[`${pair.pairKey}_system_b`] && (
                  <div className="mt-2 text-xs text-yellow-400">Validating...</div>
                )}
                
                {validationErrors[`${pair.pairKey}_system_b`] && (
                  <div className="mt-2 p-2 bg-red-900/30 border border-red-700 rounded text-xs">
                    {validationErrors[`${pair.pairKey}_system_b`].map((err, idx) => (
                      <div key={idx} className="text-red-300 mb-1">
                        <span className="font-semibold">[{err.type}]</span> Line {err.line}: {err.message}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
            )}
          </div>
        ))}
      </div>
    </>
  );
}

