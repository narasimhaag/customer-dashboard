export interface StoreEvent {
  store_id: number;
  customers_in: number;
  customers_out: number;
  time_stamp: string;
}

export interface HourlyData {
  hour: string;
  customers_in: number;
  customers_out: number;
}