var DEFAULT_SERVER = 'http://localhost:8000';

function getSettings(callback) {
  chrome.storage.sync.get({
    serverUrl: DEFAULT_SERVER,
    authToken: '',
    username: '',
    _pwd: ''
  }, function(data) {
    callback(
      data.serverUrl.replace(/\/+$/, ''),
      data.authToken,
      data.username,
      data._pwd
    );
  });
}

async function reLogin(serverUrl, username, password) {
  var resp = await fetch(serverUrl + '/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: username, password: password, client_type: 'extension' })
  });
  if (!resp.ok) return null;
  var data = await resp.json();
  if (!data.access_token) return null;
  await chrome.storage.sync.set({ authToken: data.access_token });
  return data.access_token;
}

chrome.runtime.onMessage.addListener(function(msg, sender, sendResponse) {
  if (msg.type === 'save-article') {
    handleSaveArticle(msg, sender.tab.id);
  }
});

async function handleSaveArticle(msg, tabId) {
  var title = msg.title;
  var html = msg.html;
  var url = msg.url;
  var imageUrls = msg.imageUrls;

  chrome.scripting.executeScript({
    target: { tabId: tabId },
    function: showStatus,
    args: ['Downloading ' + imageUrls.length + ' images...']
  });

  var imageMap = {};
  var downloaded = 0;
  var failed = 0;

  var promises = imageUrls.map(async function(imgUrl) {
    try {
      var resp = await fetch(imgUrl, {
        credentials: 'include',
        headers: {
          'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8'
        }
      });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      var blob = await resp.blob();
      if (blob.size < 500) throw new Error('too small');
      var reader = new FileReader();
      var dataUrl = await new Promise(function(resolve, reject) {
        reader.onloadend = function() { resolve(reader.result); };
        reader.onerror = reject;
        reader.readAsDataURL(blob);
      });
      imageMap[imgUrl] = dataUrl;
      downloaded++;
    } catch(e) {
      failed++;
    }
  });

  await Promise.all(promises);

  for (var [origUrl, dataUrl] of Object.entries(imageMap)) {
    var escaped = origUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    html = html.replace(new RegExp(escaped, 'g'), dataUrl);
  }

  chrome.scripting.executeScript({
    target: { tabId: tabId },
    function: showStatus,
    args: ['Saving (' + downloaded + '/' + imageUrls.length + ' images)...']
  });

  getSettings(async function(serverUrl, authToken, username, storedPwd) {
    if (!authToken) {
      if (username && storedPwd) {
        authToken = await reLogin(serverUrl, username, storedPwd);
      }
      if (!authToken) {
        chrome.scripting.executeScript({
          target: { tabId: tabId },
          function: showError,
          args: ['Not logged in. Please open the extension popup and login first.']
        });
        return;
      }
    }

    var payload = JSON.stringify({ title: title, html: html, source_url: url });

    async function doUpload(token) {
      return fetch(serverUrl + '/api/files/upload-web-clip', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + token
        },
        body: payload
      });
    }

    try {
      var resp = await doUpload(authToken);

      if (resp.status === 401 && username && storedPwd) {
        var newToken = await reLogin(serverUrl, username, storedPwd);
        if (newToken) {
          resp = await doUpload(newToken);
        } else {
          chrome.scripting.executeScript({
            target: { tabId: tabId },
            function: showError,
            args: ['Session expired and auto re-login failed. Please re-login in the extension popup.']
          });
          chrome.storage.sync.set({ authToken: '' });
          return;
        }
      } else if (resp.status === 401) {
        chrome.scripting.executeScript({
          target: { tabId: tabId },
          function: showError,
          args: ['Session expired. Please re-login in the extension popup.']
        });
        chrome.storage.sync.set({ authToken: '' });
        return;
      }

      var result = await resp.json();

      if (result.success) {
        chrome.scripting.executeScript({
          target: { tabId: tabId },
          function: showSuccess,
          args: [downloaded, imageUrls.length, title]
        });
      } else {
        var errMsg = result.detail || result.error || 'Unknown error';
        if (typeof errMsg !== 'string') errMsg = JSON.stringify(errMsg);
        chrome.scripting.executeScript({
          target: { tabId: tabId },
          function: showError,
          args: [errMsg]
        });
      }
    } catch(e) {
      var catchMsg = (e && e.message) ? e.message : String(e);
      chrome.scripting.executeScript({
        target: { tabId: tabId },
        function: showError,
        args: [catchMsg + '\n\nIs the server running at ' + serverUrl + '?']
      });
    }
  });
}

// --- Functions injected into the page ---

function showStatus(msg) {
  var el = document.getElementById('guanhe-clip-status');
  if (!el) {
    el = document.createElement('div');
    el.id = 'guanhe-clip-status';
    el.style.cssText = 'position:fixed;top:20px;right:20px;background:#1677ff;color:white;padding:16px 24px;border-radius:12px;font-family:-apple-system,sans-serif;font-size:15px;font-weight:600;z-index:999999;box-shadow:0 4px 20px rgba(0,0,0,0.15)';
    document.body.appendChild(el);
  }
  el.textContent = msg;
}

function showSuccess(count, total, title) {
  var el = document.getElementById('guanhe-clip-status');
  if (!el) {
    el = document.createElement('div');
    el.id = 'guanhe-clip-status';
    el.style.cssText = 'position:fixed;top:20px;right:20px;background:#52c41a;color:white;padding:16px 24px;border-radius:12px;font-family:-apple-system,sans-serif;font-size:15px;font-weight:600;z-index:999999;box-shadow:0 4px 20px rgba(0,0,0,0.15)';
    document.body.appendChild(el);
  }
  el.style.background = '#52c41a';
  el.textContent = '✓ Saved "' + (title.length > 40 ? title.substring(0, 40) + '...' : title) + '" (' + count + '/' + total + ' images)';
  setTimeout(function() {
    el.style.opacity = '0';
    el.style.transition = 'opacity 0.5s';
    setTimeout(function() { el.remove(); }, 500);
  }, 4000);
}

function showError(msg) {
  var el = document.getElementById('guanhe-clip-status');
  if (el) el.remove();
  alert('Web Clipper: Failed to save\n\n' + msg);
}
