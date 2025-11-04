// 类型声明文件：确保 TypeScript 能识别 lightweight-charts
declare module 'lightweight-charts' {
  export type Time = number | string;
  
  export interface CandlestickData<T extends Time = Time> {
    time: T;
    open: number;
    high: number;
    low: number;
    close: number;
  }
  
  export interface HistogramData<T extends Time = Time> {
    time: T;
    value: number;
    color?: string;
  }
  
  export interface IChartApi {
    remove(): void;
    applyOptions(options: any): void;
    timeScale(): any;
    addCandlestickSeries(options?: any): ISeriesApi<'Candlestick'>;
    addHistogramSeries(options?: any): ISeriesApi<'Histogram'>;
    addLineSeries(options?: any): ISeriesApi<'Line'>;
  }
  
  export interface ISeriesApi<T extends string> {
    setData(data: any[]): void;
  }
  
  export function createChart(container: HTMLElement | string, options?: any): IChartApi;
}

