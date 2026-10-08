'use strict';

// Installation only. Private pages, documents and business requests always
// use the existing server authentication, with no offline copy or replay.
self.addEventListener('install', event => {
  event.waitUntil(self.skipWaiting());
});
self.addEventListener('activate', event => {
  event.waitUntil(self.clients.claim());
});
