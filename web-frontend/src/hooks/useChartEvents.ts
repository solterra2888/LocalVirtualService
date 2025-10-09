import { useState, useEffect, useCallback } from 'react';
import { ChartEvent, EventFormData, EventType, EventPosition } from '../types/events';
import { ChartEventsAPI } from '../services/chartEvents';

export const useChartEvents = (symbol: string) => {
  const [events, setEvents] = useState<ChartEvent[]>([]);
  const [loading, setLoading] = useState(false);

  // Load events from API
  const loadEvents = useCallback(async () => {
    if (!symbol) return;
    
    setLoading(true);
    try {
      const events = await ChartEventsAPI.getEventsBySymbol(symbol);
      // Sort by date
      events.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
      setEvents(events);
    } catch (error) {
      console.error('Error loading events from API:', error);
      setEvents([]);
    } finally {
      setLoading(false);
    }
  }, [symbol]);

  // Note: No longer need saveEvents as API handles persistence

  // Add new event
  const addEvent = useCallback(async (eventData: EventFormData): Promise<void> => {
    setLoading(true);
    try {
      await ChartEventsAPI.createEvent(symbol, eventData);
      
      // Reload events from server
      await loadEvents();
    } finally {
      setLoading(false);
    }
  }, [symbol, loadEvents]);

  // Update event
  const updateEvent = useCallback(async (eventId: string, eventData: EventFormData): Promise<void> => {
    setLoading(true);
    try {
      await ChartEventsAPI.updateEvent(eventId, eventData);
      
      // Reload events from server
      await loadEvents();
    } finally {
      setLoading(false);
    }
  }, [loadEvents]);

  // Delete event
  const deleteEvent = useCallback(async (eventId: string): Promise<void> => {
    setLoading(true);
    try {
      await ChartEventsAPI.deleteEvent(eventId);
      
      // Reload events from server
      await loadEvents();
    } finally {
      setLoading(false);
    }
  }, [loadEvents]);

  // Get events by date range
  const getEventsByDateRange = useCallback((startDate: string, endDate: string): ChartEvent[] => {
    return events.filter(event => {
      const eventDate = new Date(event.date);
      const start = new Date(startDate);
      const end = new Date(endDate);
      return eventDate >= start && eventDate <= end;
    });
  }, [events]);

  // Get events by date
  const getEventsByDate = useCallback((date: string): ChartEvent[] => {
    return events.filter(event => event.date === date);
  }, [events]);

  // Check if there are events on specified date
  const hasEventOnDate = useCallback((date: string): boolean => {
    return events.some(event => event.date === date);
  }, [events]);

  // Get event statistics
  const getEventStats = useCallback(() => {
    const stats = {
      total: events.length,
      byType: {} as Record<EventType, number>,
      byPosition: {} as Record<EventPosition, number>,
    };

    events.forEach(event => {
      // Count by type
      stats.byType[event.type] = (stats.byType[event.type] || 0) + 1;
      
      // Count by position
      const position = event.position || EventPosition.MIDDLE;
      stats.byPosition[position] = (stats.byPosition[position] || 0) + 1;
    });

    return stats;
  }, [events]);

  // Clear all events (delete all events for this symbol)
  const clearAllEvents = useCallback(async (): Promise<void> => {
    setLoading(true);
    try {
      // Delete all events for this symbol
      for (const event of events) {
        await ChartEventsAPI.deleteEvent(event.id);
      }
      
      // Reload events from server
      await loadEvents();
    } finally {
      setLoading(false);
    }
  }, [events, loadEvents]);

  // Export event data
  const exportEvents = useCallback((): string => {
    return JSON.stringify(events, null, 2);
  }, [events]);

  // Import event data
  const importEvents = useCallback(async (eventsJson: string): Promise<void> => {
    setLoading(true);
    try {
      const importedEvents: ChartEvent[] = JSON.parse(eventsJson);
      
      // Convert to EventFormData and import via API
      const eventFormDataList: EventFormData[] = importedEvents
        .filter(event => event.date && event.type && event.title)
        .map(event => ({
          date: event.date,
          type: event.type as EventType,
          title: event.title,
          description: event.description,
          position: event.position as EventPosition,
          relatedUrl: event.relatedUrl,
          relatedFileId: event.relatedFileId,
        }));

      const successCount = await ChartEventsAPI.importEvents(symbol, eventFormDataList);
      
      // Reload events from server
      await loadEvents();
      
      console.log(`Successfully imported ${successCount} events`);
    } catch (error) {
      console.error('Error importing events:', error);
      throw new Error('导入数据格式错误');
    } finally {
      setLoading(false);
    }
  }, [loadEvents, symbol]);

  // Load events when component mounts
  useEffect(() => {
    if (symbol) {
      loadEvents();
    }
  }, [symbol, loadEvents]);

  return {
    events,
    loading,
    addEvent,
    updateEvent,
    deleteEvent,
    getEventsByDateRange,
    getEventsByDate,
    hasEventOnDate,
    getEventStats,
    clearAllEvents,
    exportEvents,
    importEvents,
    refreshEvents: loadEvents,
  };
};
