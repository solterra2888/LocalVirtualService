var DEFAULT_SERVER = 'http://localhost:8000';

var serverInput = document.getElementById('server-url');
var saveServerBtn = document.getElementById('save-server-btn');
var usernameInput = document.getElementById('username');
var passwordInput = document.getElementById('password');
var loginBtn = document.getElementById('login-btn');
var logoutBtn = document.getElementById('logout-btn');
var loginForm = document.getElementById('login-form');
var loggedInEl = document.getElementById('logged-in');
var displayUsername = document.getElementById('display-username');
var captureBtn = document.getElementById('capture-btn');
var statusEl = document.getElementById('status');
var toggleServer = document.getElementById('toggle-server');
var serverSection = document.getElementById('server-section');
var rememberPasswordCheckbox = document.getElementById('remember-password');

// Toggle server settings visibility
toggleServer.addEventListener('click', function() {
  serverSection.classList.toggle('visible');
});

// Load saved settings
chrome.storage.sync.get({
  serverUrl: DEFAULT_SERVER,
  authToken: '',
  username: '',
  rememberPassword: true,
  _pwd: ''
}, function(data) {
  serverInput.value = data.serverUrl;
  rememberPasswordCheckbox.checked = data.rememberPassword !== false;
  if (data.authToken && data.username) {
    showLoggedIn(data.username);
  } else if (data.rememberPassword !== false) {
    if (data.username) usernameInput.value = data.username;
    if (data._pwd) passwordInput.value = data._pwd;
  }
});

// Save server URL
saveServerBtn.addEventListener('click', function() {
  var url = serverInput.value.trim().replace(/\/+$/, '');
  if (!url) {
    showStatus('URL cannot be empty', 'error');
    return;
  }
  chrome.storage.sync.set({ serverUrl: url }, function() {
    showStatus('Server URL saved', 'success');
  });
});

// Login
loginBtn.addEventListener('click', function() {
  var user = usernameInput.value.trim();
  var pass = passwordInput.value;
  if (!user || !pass) {
    showStatus('Please enter username and password', 'error');
    return;
  }

  loginBtn.textContent = 'Logging in...';
  loginBtn.disabled = true;

  var serverUrl = serverInput.value.trim().replace(/\/+$/, '');

  fetch(serverUrl + '/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: user, password: pass, client_type: 'extension' })
  })
  .then(function(resp) {
    if (!resp.ok) throw new Error('Login failed (HTTP ' + resp.status + ')');
    return resp.json();
  })
  .then(function(data) {
    if (data.access_token) {
      var remember = rememberPasswordCheckbox.checked;
      var payload = {
        authToken: data.access_token,
        username: user,
        serverUrl: serverUrl,
        rememberPassword: remember
      };
      payload._pwd = remember ? pass : '';
      chrome.storage.sync.set(payload, function() {
        showLoggedIn(user);
        showStatus('Login successful', 'success');
      });
    } else {
      throw new Error('No access token returned');
    }
  })
  .catch(function(err) {
    showStatus(err.message, 'error');
  })
  .finally(function() {
    loginBtn.textContent = 'Login';
    loginBtn.disabled = false;
  });
});

// Logout
logoutBtn.addEventListener('click', function() {
  chrome.storage.sync.get(['rememberPassword'], function(r) {
    var remember = r.rememberPassword !== false;
    var patch = { authToken: '' };
    if (!remember) {
      patch.username = '';
      patch._pwd = '';
    }
    chrome.storage.sync.set(patch, function() {
      showLoggedOut();
      showStatus('Logged out', 'success');
    });
  });
});

// Unchecking "Remember password" clears stored password immediately
rememberPasswordCheckbox.addEventListener('change', function() {
  if (!this.checked) {
    chrome.storage.sync.set({ rememberPassword: false, _pwd: '' });
    passwordInput.value = '';
  } else {
    chrome.storage.sync.set({ rememberPassword: true });
  }
});

// Capture current page
captureBtn.addEventListener('click', function() {
  chrome.tabs.query({ active: true, currentWindow: true }, function(tabs) {
    if (!tabs[0]) return;
    statusEl.textContent = 'Capturing...';
    statusEl.className = 'status';
    var tabId = tabs[0].id;
    chrome.scripting.executeScript({
      target: { tabId: tabId },
      files: ['Readability.js']
    }, function() {
      chrome.scripting.executeScript({
        target: { tabId: tabId },
        function: extractContent
      });
    });
    setTimeout(function() { window.close(); }, 800);
  });
});

// Allow Enter key in password field to trigger login
passwordInput.addEventListener('keydown', function(e) {
  if (e.key === 'Enter') loginBtn.click();
});

function showLoggedIn(user) {
  loginForm.style.display = 'none';
  loggedInEl.style.display = 'flex';
  displayUsername.textContent = user;
  captureBtn.disabled = false;
}

function showLoggedOut() {
  loginForm.style.display = 'block';
  loggedInEl.style.display = 'none';
  displayUsername.textContent = '';
  captureBtn.disabled = true;
  chrome.storage.sync.get(['rememberPassword', 'username', '_pwd'], function(d) {
    var remember = d.rememberPassword !== false;
    rememberPasswordCheckbox.checked = remember;
    if (remember && d.username) {
      usernameInput.value = d.username;
      passwordInput.value = d._pwd || '';
    } else {
      usernameInput.value = '';
      passwordInput.value = '';
    }
  });
}

function showStatus(msg, type) {
  statusEl.textContent = msg;
  statusEl.className = 'status ' + (type || '');
  if (type === 'success') {
    setTimeout(function() { statusEl.textContent = ''; }, 3000);
  }
}

// Content extraction function — injected into the active tab
// Readability.js is injected before this function runs
function extractContent() {
  var u = location.href;

  var statusEl = document.createElement('div');
  statusEl.id = 'guanhe-clip-status';
  statusEl.style.cssText = 'position:fixed;top:20px;right:20px;background:#1677ff;color:white;padding:16px 24px;border-radius:12px;font-family:-apple-system,sans-serif;font-size:15px;font-weight:600;z-index:999999;box-shadow:0 4px 20px rgba(0,0,0,0.15)';
  statusEl.textContent = 'Extracting content...';
  document.body.appendChild(statusEl);

  // --- Phase 1: Resolve lazy-loaded images on the LIVE DOM before cloning ---
  var liveImgs = document.querySelectorAll('img');
  for (var li = 0; li < liveImgs.length; li++) {
    var img = liveImgs[li];
    var real = img.currentSrc || img.src;
    if (!real || real === 'about:blank' || real.endsWith('/blank.gif') || real.includes('data:image/gif') || real.includes('data:image/svg')) {
      real = img.getAttribute('data-src') ||
             img.getAttribute('data-lazy-src') ||
             img.getAttribute('data-original') ||
             img.getAttribute('data-full-url') ||
             img.getAttribute('data-image') ||
             img.getAttribute('data-hi-res-src') ||
             img.getAttribute('data-large-file') ||
             img.dataset.src || '';
    }
    if (!real || real === 'about:blank') {
      var srcset = img.getAttribute('srcset') || img.getAttribute('data-srcset') || '';
      if (srcset) {
        var candidates = srcset.split(',').map(function(s) {
          var parts = s.trim().split(/\s+/);
          return { url: parts[0], desc: parseFloat(parts[1]) || 0 };
        }).filter(function(c) { return c.url; });
        candidates.sort(function(a, b) { return b.desc - a.desc; });
        if (candidates.length > 0) real = candidates[0].url;
      }
    }
    if (real && !real.startsWith('data:')) {
      try { real = new URL(real, location.origin).href; } catch(e) { continue; }
      if (real.includes('/_next/image')) {
        try {
          var imgUrl = new URL(real);
          var actualUrl = imgUrl.searchParams.get('url');
          if (actualUrl) {
            real = actualUrl.startsWith('http') ? actualUrl : new URL(actualUrl, location.origin).href;
          }
        } catch(e) {}
      }
      img.setAttribute('src', real);
    }
  }

  // --- Phase 2: Use Readability to extract main article content ---
  var noiseSelectors = [
    'nav', 'footer', 'aside',
    '[role="banner"]', '[role="navigation"]', '[role="complementary"]',
    '[data-test-id="right-rail"]',
    '[data-test-id="recommended-section"]',
    '[data-test-id="recommended-for-you-card-body"]',
    '[data-test-id="trending-card-container"]',
    '[data-test-id="trending-news-cards-container"]',
    '[data-test-id="comments-section"]',
    '[data-test-id="suggested-symbols-card"]',
    '[data-test-id="about-symbol-card"]',
    '[data-test-id="post-footer"]',
    '[data-test-id="action-block-top"]',
    '.signup_widget_placeholder',
    '.sidebar', '.comments', '.related', '.recommended', '.trending',
    '.ad', '.advertisement', '#comments', '.comment-section',
    '#more-links'
  ];
  var noiseSelector = noiseSelectors.join(', ');

  var t, articleHtml, byline, publishedTime, siteName;
  if (typeof Readability !== 'undefined') {
    var docClone = document.cloneNode(true);
    docClone.querySelectorAll(noiseSelector).forEach(function(el) { el.remove(); });
    var reader = new Readability(docClone, {
      charThreshold: 50,
      keepClasses: false
    });
    var article = reader.parse();
    if (article && article.content) {
      t = article.title || document.title;
      byline = article.byline || '';
      publishedTime = article.publishedTime || '';
      siteName = article.siteName || '';
      articleHtml = article.content;
    }
  }

  // Fallback: if Readability fails or is unavailable, use selector-based approach
  if (!articleHtml) {
    var selectors = ['[data-test-id="content-container"]', 'article', '.post-content', '[role=main]', 'main'];
    var c = null;
    for (var si = 0; si < selectors.length; si++) {
      var el = document.querySelector(selectors[si]);
      if (el) { c = el; break; }
    }
    if (!c) c = document.body;
    var cl = c.cloneNode(true);
    cl.querySelectorAll(noiseSelector).forEach(function(el) { el.remove(); });
    cl.querySelectorAll('video, iframe, script, style').forEach(function(el) { el.remove(); });
    articleHtml = cl.innerHTML;
    t = document.title;
  }

  t = (t || document.title)
    .replace(/^\(\d+\)\s*/, '')
    .replace(/\s*[-–|](?:\s*by\s*)?\s*[^-–|]*$/i, '')
    .trim();

  // Try to find published date from page metadata if Readability didn't get it
  if (!publishedTime) {
    var dateMeta = document.querySelector('meta[property="article:published_time"]') ||
                   document.querySelector('meta[name="date"]') ||
                   document.querySelector('meta[name="pubdate"]') ||
                   document.querySelector('time[datetime]');
    if (dateMeta) {
      publishedTime = dateMeta.getAttribute('content') || dateMeta.getAttribute('datetime') || '';
    }
  }
  if (!byline) {
    var authorMeta = document.querySelector('meta[name="author"]') ||
                     document.querySelector('meta[property="article:author"]');
    if (authorMeta) byline = authorMeta.getAttribute('content') || '';
  }

  // Prepend article header (title + metadata) to the article HTML
  var headerHtml = '<h1>' + t.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</h1>';
  var metaParts = [];
  if (publishedTime) {
    try {
      var d = new Date(publishedTime);
      if (!isNaN(d.getTime())) {
        metaParts.push(d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }));
      } else {
        metaParts.push(publishedTime);
      }
    } catch(e) { metaParts.push(publishedTime); }
  }
  if (byline) metaParts.push(byline.replace(/</g, '&lt;'));
  if (siteName) metaParts.push(siteName.replace(/</g, '&lt;'));
  if (metaParts.length > 0) {
    headerHtml += '<p style="color:#666;font-size:14px;">' + metaParts.join(' · ') + '</p>';
  }
  articleHtml = headerHtml + articleHtml;

  // --- Phase 3: Extract image URLs from the cleaned article HTML ---
  var tempDiv = document.createElement('div');
  tempDiv.innerHTML = articleHtml;
  var imageUrls = [];
  var articleImgs = tempDiv.querySelectorAll('img');
  for (var i = 0; i < articleImgs.length; i++) {
    var src = articleImgs[i].getAttribute('src') || '';
    if (src && !src.startsWith('data:')) {
      try { src = new URL(src, location.origin).href; } catch(e) { continue; }
      articleImgs[i].setAttribute('src', src);
      if (imageUrls.indexOf(src) === -1) imageUrls.push(src);
    }
    articleImgs[i].removeAttribute('srcset');
    articleImgs[i].removeAttribute('loading');
    articleImgs[i].removeAttribute('data-src');
    articleImgs[i].removeAttribute('data-lazy-src');
    articleImgs[i].removeAttribute('data-srcset');
  }

  // Remove <picture> <source> elements, keep <img>
  tempDiv.querySelectorAll('picture source').forEach(function(s) { s.remove(); });

  var h = tempDiv.innerHTML;
  if (h.length > 20000000) h = h.substring(0, 20000000);

  statusEl.textContent = 'Found ' + imageUrls.length + ' images, downloading...';

  chrome.runtime.sendMessage({
    type: 'save-article',
    title: t,
    html: h,
    url: u,
    imageUrls: imageUrls,
    // 结构化元数据 (2026-11-22): 服务端转投 subscription_content 用,
    // published_time 为原始 ISO 串 (Readability publishedTime / meta 兜底)
    publishedTime: publishedTime || '',
    byline: byline || '',
    siteName: siteName || ''
  });
}
