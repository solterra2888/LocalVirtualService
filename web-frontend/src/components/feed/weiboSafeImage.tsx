const WEIBO_CDN_RE = /sinaimg\.cn/i;

/**
 * Rewrite sinaimg.cn URLs to same-origin backend proxy.
 * Weibo CDN blocks production Referer and also blocks browser UA without weibo.com Referer.
 */
export function weiboSafeImageUrl(url?: string | null): string | undefined {
  if (!url) return undefined;
  if (WEIBO_CDN_RE.test(url)) {
    return `/api/subscriptions/proxy-image?url=${encodeURIComponent(url)}`;
  }
  return url;
}

/** Avatar `src` — returns a plain string (no embedded img element). */
export function weiboSafeAvatarSrc(url?: string | null): string | undefined {
  return weiboSafeImageUrl(url);
}
