import { useState, useEffect } from 'react';

/**
 * Custom hook for data fetching with loading and error states
 */
export function useFetch(url, deps = []) {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const refresh = () => {
    setLoading(true);
    setError(null);
    fetch(url)
      .then(async (r) => {
        if (!r.ok) {
          const text = await r.text().catch(() => "");
          let action = null;
          let message = null;
          try {
            const parsed = JSON.parse(text);
            if (parsed.action) action = parsed.action;
            if (parsed.message) message = parsed.message;
          } catch {}
          
          // If this is a setup_required error, throw a special error
          if (action === "setup_required") {
            const err = new Error(message || "Database not initialized");
            err.action = "setup_required";
            throw err;
          }
          
          throw new Error(`${r.status} ${r.statusText}: ${text || "Request failed"}`);
        }
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e : new Error(String(e))))
      .finally(() => setLoading(false));
  };
  
  useEffect(refresh, deps);
  
  const clearError = () => setError(null);
  
  return { data, loading, error, refresh, clearError };
}

