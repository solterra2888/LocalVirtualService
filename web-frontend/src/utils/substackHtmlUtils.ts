import DOMPurify from 'dompurify';

/** Substack 正文里的 DOM 组件：整段删除 */
const REMOVE_COMPONENTS = new Set([
  'PaywallToDOM',
  'SubscribeWidgetToDOM',
  'ButtonCreateButton',
  'CommunityPostPlaceholder',
]);

/** 无内容占位 embed：删除空节点 */
const EMPTY_EMBED_COMPONENTS = new Set([
  'Twitter2ToDOM',
  'MentionToDOM',
  'CashtagToDOM',
  'EmbeddedPostToDOM',
]);

function extractImageUrl(root: Element): string | null {
  const img = root.querySelector('img[src]');
  const imgSrc = img?.getAttribute('src')?.trim();
  if (imgSrc) return imgSrc;

  const link = root.matches('a.image-link')
    ? root
    : root.querySelector('a.image-link[href]');
  const href = link?.getAttribute('href')?.trim();
  if (href) return href;

  const source = root.querySelector('source[srcset]');
  const srcset = source?.getAttribute('srcset')?.trim();
  if (srcset) {
    return srcset.split(',')[0]?.trim().split(/\s+/)[0] || null;
  }
  return null;
}

function replaceImageBlock(el: Element, doc: Document): void {
  const url = extractImageUrl(el);
  if (!url) return;

  const container =
    el.closest('.captioned-image-container') ||
    el.closest('figure') ||
    el;

  const img = doc.createElement('img');
  img.setAttribute('src', url);
  img.setAttribute('alt', '');
  img.setAttribute('loading', 'lazy');
  container.replaceWith(img);
}

function simplifySubstackDom(doc: Document): void {
  doc.querySelectorAll('.subscription-widget, .paywall-jump').forEach((el) => el.remove());
  doc.querySelectorAll('svg.lucide').forEach((el) => el.remove());

  Array.from(doc.querySelectorAll('[data-component-name="Image2ToDOM"]')).forEach((el) => {
    replaceImageBlock(el, doc);
  });

  Array.from(doc.querySelectorAll('[data-component-name]')).forEach((el) => {
    const name = el.getAttribute('data-component-name') || '';

    if (REMOVE_COMPONENTS.has(name)) {
      el.remove();
      return;
    }

    if (EMPTY_EMBED_COMPONENTS.has(name)) {
      const text = (el.textContent || '').replace(/\s+/g, '').trim();
      const hasMedia = el.querySelector('img, iframe, video, audio, blockquote, table');
      if (!text && !hasMedia) {
        el.remove();
      }
    }
  });

  doc.querySelectorAll('.image-link-expand, .pc-display-flex.pc-gap-8').forEach((el) => {
    if (!(el.textContent || '').trim()) el.remove();
  });

  doc.querySelectorAll('p, div').forEach((el) => {
    if ((el.textContent || '').trim()) return;
    if (el.querySelector('img, iframe, video, audio, table, hr')) return;
    el.remove();
  });
}

/**
 * 清洗 Substack 文章 HTML，供详情页渲染。
 * 去掉 paywall / 订阅 widget / 空 embed，并把 Image2 块简化为 <img>。
 */
export function prepareSubstackHtmlForDisplay(rawHtml: string): string {
  const trimmed = (rawHtml || '').trim();
  if (!trimmed) return '';

  const doc = new DOMParser().parseFromString(trimmed, 'text/html');
  simplifySubstackDom(doc);

  return DOMPurify.sanitize(doc.body.innerHTML, {
    ADD_TAGS: ['iframe'],
    ADD_ATTR: ['target', 'rel', 'allow', 'allowfullscreen', 'frameborder', 'loading'],
  });
}

/** 列表摘要、阅读时长估算用的纯文本 */
export function substackHtmlToPlainText(rawHtml: string): string {
  const trimmed = (rawHtml || '').trim();
  if (!trimmed) return '';

  const doc = new DOMParser().parseFromString(trimmed, 'text/html');
  simplifySubstackDom(doc);
  return (doc.body.textContent || '').replace(/\s+/g, ' ').trim();
}
