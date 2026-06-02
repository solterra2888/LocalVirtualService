/**
 * CRA dev-server proxy (http-proxy-middleware v1, bundled with react-scripts 5).
 *
 * This form (app.use + path first argument) is the ONLY correct pattern for
 * react-scripts 5.  The pathFilter option is v2-only and must NOT be used here.
 *
 * webpack-dev-server preserves req.url (does not strip the /api mount prefix)
 * so the backend receives the full path e.g. /api/theme-report/latest — matching
 * the FastAPI routes which are mounted at prefix="/api" in main.py.
 *
 * Why this file instead of "proxy" in package.json?
 * The simple "proxy" string skips requests with Accept: text/html (browser
 * navigation).  That breaks <iframe src="/api/theme-report/shared/..."> because
 * the browser sends Accept: text/html for iframe navigation.  This file registers
 * the proxy BEFORE webpack's SPA fallback so all /api/ requests are forwarded,
 * including iframe navigation.
 */

const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function (app) {
  app.use(
    '/api',
    createProxyMiddleware({
      target: 'http://localhost:8000',
      changeOrigin: true,
    })
  );
};
