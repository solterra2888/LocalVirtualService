import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { detectBrowserTimezone, USER_TZ_STORAGE_KEY } from '../utils/datetime';

const DASHBOARD_THEME_KEY = 'finviz-dashboard-theme';

export type DashboardThemeMode = 'light' | 'dark';

function getStoredTheme(): DashboardThemeMode {
  try {
    const v = localStorage.getItem(DASHBOARD_THEME_KEY);
    if (v === 'light' || v === 'dark') return v;
  } catch (_) {}
  return 'dark';
}

/** 用户的展示时区:
 *  - 优先 localStorage 中的偏好（用户在 Settings 里手动设的）
 *  - 否则使用浏览器自动检测的 IANA 时区（中国默认 'Asia/Shanghai'，美国默认 'America/New_York' 等）
 *  - 最终回退 'UTC'
 *
 * 注意: 这是"展示时区", 不影响后端存储 (后端永远 UTC).
 */
function getStoredUserTimezone(): string {
  try {
    const v = localStorage.getItem(USER_TZ_STORAGE_KEY);
    if (v && typeof v === 'string') return v;
  } catch (_) {}
  return detectBrowserTimezone();
}

interface DashboardSettingsContextValue {
  themeMode: DashboardThemeMode;
  setThemeMode: (mode: DashboardThemeMode) => void;
  userTimezone: string;
  setUserTimezone: (tz: string) => void;
  /** 重置为浏览器自动检测的时区. */
  resetUserTimezone: () => void;
}

const DashboardSettingsContext = createContext<DashboardSettingsContextValue | null>(null);

export const DashboardSettingsProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [themeMode, setThemeModeState] = useState<DashboardThemeMode>(getStoredTheme);
  const [userTimezone, setUserTimezoneState] = useState<string>(getStoredUserTimezone);

  const setThemeMode = useCallback((mode: DashboardThemeMode) => {
    setThemeModeState(mode);
    try {
      localStorage.setItem(DASHBOARD_THEME_KEY, mode);
    } catch (_) {}
  }, []);

  const setUserTimezone = useCallback((tz: string) => {
    setUserTimezoneState(tz);
    try {
      localStorage.setItem(USER_TZ_STORAGE_KEY, tz);
    } catch (_) {}
  }, []);

  const resetUserTimezone = useCallback(() => {
    const auto = detectBrowserTimezone();
    setUserTimezoneState(auto);
    try {
      localStorage.removeItem(USER_TZ_STORAGE_KEY);
    } catch (_) {}
  }, []);

  useEffect(() => {
    setThemeModeState(getStoredTheme());
    setUserTimezoneState(getStoredUserTimezone());
  }, []);

  const value: DashboardSettingsContextValue = {
    themeMode,
    setThemeMode,
    userTimezone,
    setUserTimezone,
    resetUserTimezone,
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
      userTimezone: getStoredUserTimezone(),
      setUserTimezone: () => {},
      resetUserTimezone: () => {},
    };
  }
  return ctx;
}
