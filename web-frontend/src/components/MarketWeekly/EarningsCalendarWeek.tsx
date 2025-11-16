import React from 'react';
import { Empty, Tooltip } from 'antd';
import type { EarningsEvent } from '../../types/marketWeekly';
import './EarningsCalendarWeek.css';

interface EarningsCalendarWeekProps {
  data: EarningsEvent[];
}

interface DayEarnings {
  date: string;
  dayName: string;
  beforeOpen: EarningsEvent[];
  afterClose: EarningsEvent[];
}

// 单色矢量太阳图标
const SunIcon: React.FC = () => (
  <svg
    className="section-icon"
    viewBox="0 0 24 24"
    aria-hidden="true"
  >
    <circle cx="12" cy="12" r="4" stroke="currentColor" strokeWidth="1.5" fill="none" />
    <line x1="12" y1="2" x2="12" y2="5" stroke="currentColor" strokeWidth="1.5" />
    <line x1="12" y1="19" x2="12" y2="22" stroke="currentColor" strokeWidth="1.5" />
    <line x1="4.22" y1="4.22" x2="6.34" y2="6.34" stroke="currentColor" strokeWidth="1.5" />
    <line x1="17.66" y1="17.66" x2="19.78" y2="19.78" stroke="currentColor" strokeWidth="1.5" />
    <line x1="2" y1="12" x2="5" y2="12" stroke="currentColor" strokeWidth="1.5" />
    <line x1="19" y1="12" x2="22" y2="12" stroke="currentColor" strokeWidth="1.5" />
    <line x1="4.22" y1="19.78" x2="6.34" y2="17.66" stroke="currentColor" strokeWidth="1.5" />
    <line x1="17.66" y1="6.34" x2="19.78" y2="4.22" stroke="currentColor" strokeWidth="1.5" />
  </svg>
);

// 单色矢量月亮图标
const MoonIcon: React.FC = () => (
  <svg
    className="section-icon"
    viewBox="0 0 24 24"
    aria-hidden="true"
  >
    <path
      d="M20 13.5A7.5 7.5 0 0 1 11.5 5 6.5 6.5 0 1 0 20 13.5Z"
      stroke="currentColor"
      strokeWidth="1.5"
      fill="none"
    />
  </svg>
);

export const EarningsCalendarWeek: React.FC<EarningsCalendarWeekProps> = ({ data }) => {
  // 获取下周的周一到周五日期
  const getNextWeekDays = (): Date[] => {
    const today = new Date();
    const currentDay = today.getDay(); // 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    
    // 计算到下周一的天数
    let daysUntilNextMonday = 1 - currentDay; // 如果今天是周日，则为1；如果是周一，则为-6
    if (daysUntilNextMonday <= 0) {
      daysUntilNextMonday += 7; // 如果已经过了本周一，则跳到下周一
    }
    
    const nextMonday = new Date(today);
    nextMonday.setDate(today.getDate() + daysUntilNextMonday);
    nextMonday.setHours(0, 0, 0, 0);
    
    // 生成周一到周五
    const weekDays: Date[] = [];
    for (let i = 0; i < 5; i++) {
      const day = new Date(nextMonday);
      day.setDate(nextMonday.getDate() + i);
      weekDays.push(day);
    }
    
    return weekDays;
  };

  // 按日期分组并分类，确保周一到周五都有
  const groupByDay = (): DayEarnings[] => {
    // 获取下周的周一到周五
    const weekDates = getNextWeekDays();
    
    // 如果有数据，找出数据中的第一个日期来确定是哪一周
    if (data && data.length > 0) {
      const firstEventDate = new Date(data[0].date);
      firstEventDate.setHours(0, 0, 0, 0);
      
      // 找到该日期所在周的周一
      const dayOfWeek = firstEventDate.getDay();
      const daysFromMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1; // 周日算作6天
      const monday = new Date(firstEventDate);
      monday.setDate(firstEventDate.getDate() - daysFromMonday);
      
      // 重新生成这一周的周一到周五
      weekDates.length = 0;
      for (let i = 0; i < 5; i++) {
        const day = new Date(monday);
        day.setDate(monday.getDate() + i);
        weekDates.push(day);
      }
    }
    
    // 初始化所有工作日的数据结构
    const dateMap = new Map<string, { beforeOpen: EarningsEvent[]; afterClose: EarningsEvent[] }>();
    weekDates.forEach(date => {
      const dateStr = date.toISOString().split('T')[0];
      dateMap.set(dateStr, { beforeOpen: [], afterClose: [] });
    });
    
    // 填充实际的 earnings 数据
    if (data && data.length > 0) {
      data.forEach(event => {
        if (!dateMap.has(event.date)) {
          // 如果数据中有周末的日期，跳过
          return;
        }
        
        const group = dateMap.get(event.date)!;
        const timeUpper = event.time.toUpperCase();
        
        if (timeUpper.includes('BEFORE')) {
          group.beforeOpen.push(event);
        } else if (timeUpper.includes('AFTER')) {
          group.afterClose.push(event);
        } else {
          // 默认归类到 after close
          group.afterClose.push(event);
        }
      });
    }

    // 转换为数组
    const days: DayEarnings[] = [];
    weekDates.forEach(date => {
      const dateStr = date.toISOString().split('T')[0];
      const dayName = date.toLocaleDateString('en-US', { weekday: 'short' }).toUpperCase();
      const dayNumber = date.getDate();
      
      const group = dateMap.get(dateStr)!;
      days.push({
        date: dayNumber.toString(),
        dayName,
        beforeOpen: group.beforeOpen,
        afterClose: group.afterClose
      });
    });

    return days;
  };

  const weekDays = groupByDay();

  return (
    <div className="earnings-calendar-week">
      {weekDays.map((day, index) => (
        <div key={index} className="earnings-day-column">
          <div className="day-header">
            <div className="day-name">{day.dayName}</div>
            <div className="day-number">{day.date}</div>
          </div>

          {day.beforeOpen.length > 0 && (
            <div className="earnings-section before-open">
              <div className="section-header">
                <SunIcon />
                <span className="calendar-section-title">Before Open</span>
              </div>
              <div className="earnings-list">
                {day.beforeOpen.map((event, idx) => (
                  <Tooltip key={idx} title={`${event.ticker} — ${event.company}`}>
                    <div className="earnings-card">
                      <div className="ticker-logo">
                        {event.logo_url ? (
                          <img
                            src={event.logo_url}
                            alt={event.ticker}
                            className="company-logo-img"
                            onError={(e) => {
                              // 如果图标加载失败，隐藏图片，保留下面的公司代码
                              e.currentTarget.style.display = 'none';
                            }}
                          />
                        ) : (
                          <span className="ticker-logo-text">{event.ticker}</span>
                        )}
                      </div>
                      <div className="ticker-label">{event.ticker}</div>
                    </div>
                  </Tooltip>
                ))}
              </div>
            </div>
          )}

          {day.beforeOpen.length === 0 && (
            <div className="earnings-section no-earnings">
              <div className="section-header">
                <SunIcon />
                <span className="calendar-section-title">Before Open</span>
              </div>
              <div className="no-earnings-message">
                <span className="no-earnings-icon">📋</span>
                <span>No Earnings</span>
              </div>
            </div>
          )}

          {day.afterClose.length > 0 && (
            <div className="earnings-section after-close">
              <div className="section-header">
                <MoonIcon />
                <span className="calendar-section-title">After Close</span>
              </div>
              <div className="earnings-list">
                {day.afterClose.map((event, idx) => (
                  <Tooltip key={idx} title={`${event.ticker} — ${event.company}`}>
                    <div className="earnings-card">
                      <div className="ticker-logo">
                        {event.logo_url ? (
                          <img
                            src={event.logo_url}
                            alt={event.ticker}
                            className="company-logo-img"
                            onError={(e) => {
                              e.currentTarget.style.display = 'none';
                            }}
                          />
                        ) : (
                          <span className="ticker-logo-text">{event.ticker}</span>
                        )}
                      </div>
                      <div className="ticker-label">{event.ticker}</div>
                    </div>
                  </Tooltip>
                ))}
              </div>
            </div>
          )}

          {day.afterClose.length === 0 && (
            <div className="earnings-section no-earnings">
              <div className="section-header">
                <MoonIcon />
                <span className="calendar-section-title">After Close</span>
              </div>
              <div className="no-earnings-message">
                <span className="no-earnings-icon">📋</span>
                <span>No Earnings</span>
              </div>
            </div>
          )}
        </div>
      ))}
    </div>
  );
};

