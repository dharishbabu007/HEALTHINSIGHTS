import { Injectable, EventEmitter } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpHeaders, HttpParams  } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

export type ErrorHandler = (res: HttpErrorResponse) => Observable<{}>;

class CacheItem<T> {
  url: string;
  timestampCached: number;
  data: T;
}

@Injectable({
  providedIn: 'root'
})
export class CachedHttpClient {
  cache: CacheItem<any>[] = [];
  headers: HttpHeaders;
  options: {};
  forceLogout: EventEmitter<boolean> = new EventEmitter();
  constructor(
    private http: HttpClient,
  ) { }

  get<T>(url: string, forceRefresh = false )
  : Observable<T> {
    let cachedItem: CacheItem<T> = this.getCachedItem<T>(url);
    forceRefresh = (forceRefresh) ? forceRefresh : false;
    const cacheTime =  86400000;
    if (cachedItem !== undefined && !forceRefresh) {
      const expireDate = cachedItem.timestampCached + cacheTime;
      if (Date.now() < expireDate) {
        return of(cachedItem.data);
      }
    }

    return this.http.get<T>(url).pipe(
      map(data => {
        if (cacheTime) { // if we actually want to cache the result
          if (cachedItem === undefined) {
            cachedItem = new CacheItem();
            cachedItem.url = url;
            this.cache.push(cachedItem);
          }
          cachedItem.data = data;
          cachedItem.timestampCached = Date.now();
        }
        return data;
      })
    );
  }

  private getCachedItem<T>(url: string): CacheItem<T> {
    return this.cache.find(item => item.url === url);
  }
  private handleError: ErrorHandler = (error: HttpErrorResponse): Observable<{}> => {
    if (error.status === 401) {
        this.forceLogout.next(true);
        return Observable.throw(error);
    } else {
      let errorMsg: string;
      if (error.error && error.error.message) {
        errorMsg = error.error.message;
      } else {
        errorMsg = (error.message) ? error.message : `${error.status} - ${error.statusText}`;
      }
      return Observable.throw(errorMsg);
    }
  }
  private handleResponse = (res: {}) => {
    console.log('ers ', res);
    return res || {};
  }

  post(url: string, param: {}, errorHandler: ErrorHandler  = this.handleError): Observable<{}> {
    const responseType = 'json';
    let body = param;
    this.headers = new HttpHeaders({
        'Content-Type': 'application/json'
    });
    body = JSON.stringify(param);
    this.options = {
        headers: this.headers,
        responseType: responseType,
        withCredentials: true
    };

    return this.http.post(url, body, this.options).pipe(
        map(data =>
            this.handleResponse(data)),
        catchError(error =>
          errorHandler(error)));
    }
    put(url: string, param: {}, errorHandler: ErrorHandler  = this.handleError): Observable<{}> {
      const responseType = 'json';
      let body = param;
      this.headers = new HttpHeaders({
          'Content-Type': 'application/json'
      });
      body = JSON.stringify(param);
      this.options = {
          headers: this.headers,
          responseType: responseType,
          withCredentials: true
      };
      return this.http.put(url, body, this.options).pipe(
          map(data =>
              this.handleResponse(data)),
          catchError(error =>
            errorHandler(error)));
      }
}
