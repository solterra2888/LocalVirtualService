const WEIBO_CDN_RE = /sinaimg\.cn/i;
const ZSXQ_CDN_RE = /(?:^|\.)images\.zsxq\.com/i;

function needsCdnProxy(url: string): boolean {
  try {
    const host = new URL(url).hostname;
    return WEIBO_CDN_RE.test(host) || ZSXQ_CDN_RE.test(host);
  } catch {
    return WEIBO_CDN_RE.test(url) || /images\.zsxq\.com/i.test(url);
  }
}

/**
 * Rewrite Weibo / ZSXQ CDN URLs to same-origin backend proxy.
 * Both CDNs block production Referer (ZSXQ returns a hotlink placeholder image).
 */
export function weiboSafeImageUrl(url?: string | null): string | undefined {
  if (!url) return undefined;
  if (needsCdnProxy(url)) {
    return `/api/subscriptions/proxy-image?url=${encodeURIComponent(url)}`;
  }
  return url;
}

/** Avatar `src` — returns a plain string (no embedded img element). */
export function weiboSafeAvatarSrc(url?: string | null): string | undefined {
  return weiboSafeImageUrl(url);
}
