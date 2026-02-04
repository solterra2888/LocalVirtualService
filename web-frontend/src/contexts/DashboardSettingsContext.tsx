import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

const DASHBOARD_THEME_KEY = 'finviz-dashboard-theme';

export type DashboardThemeMode = 'light' | 'dark';

function getStoredTheme(): DashboardThemeMode {
  try {
    const v = localStorage.getItem(DASHBOARD_THEME_KEY);
    if (v === 'light' || v === 'dark') return v;
  } catch (_) {}
  return 'dark';
}

interface DashboardSettingsContextValue {
  themeMode: DashboardThemeMode;
  setThemeMode: (mode: DashboardThemeMode) => void;
}

const DashboardSettingsContext = createContext<DashboardSettingsContextValue | null>(null);

export const DashboardSettingsProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [themeMode, setThemeModeState] = useState<DashboardThemeMode>(getStoredTheme);

  const setThemeMode = useCallback((mode: DashboardThemeMode) => {
    setThemeModeState(mode);
    try {
      localStorage.setItem(DASHBOARD_THEME_KEY, mode);
    } catch (_) {}
  }, []);

  useEffect(() => {
    const storedTheme = getStoredTheme();
    setThemeModeState(storedTheme);
  }, []);

  const value: DashboardSettingsContextValue = {
    themeMode,
    setThemeMode,
  };

  return (
    <DashboardSettingsContext.Provider value={value}>
      {children}
    </DashboardSettingsContext.Provider>
  );
};

export function useDashboardSettings(): DashboardSettingsContextValue {
  const ctx = useContext(DashboardSettingsContext);
  if (!ctx) {
    return {
      themeMode: getStoredTheme(),
      setThemeMode: () => {},
    };
  }
  return ctx;
}
