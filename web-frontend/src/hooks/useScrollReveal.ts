import { useEffect, useState } from 'react';

/**
 * 滚动渐入：观察页面内所有 `.reveal` 元素，进入视口时为其添加 `.is-visible`，
 * 触发 CSS 过渡（opacity + translateY）。卸载时自动 disconnect。
 *
 * 用法：组件挂载后调用一次 `useScrollReveal()`，给需要渐入的元素加 `className="reveal"`。
 * 若浏览器不支持 IntersectionObserver，则直接全部显示（降级）。
 */
export function useScrollReveal(): void {
  useEffect(() => {
    const els = Array.from(
      document.querySelectorAll<HTMLElement>('.reveal:not(.is-visible)')
    );
    if (els.length === 0) return;

    if (!('IntersectionObserver' in window)) {
      els.forEach((el) => el.classList.add('is-visible'));
      return;
    }

    const io = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add('is-visible');
            io.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.12, rootMargin: '0px 0px -8% 0px' }
    );

    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);
}

/**
 * 顶栏滚动状态：页面向下滚动超过 threshold 时返回 true，
 * 用于 Nav 在「透明」与「实色毛玻璃」之间切换。
 */
export function useScrolled(threshold = 24): boolean {
  const [scrolled, setScrolled] = useState(false);
  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > threshold);
    onScroll();
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
  }, [threshold]);
  return scrolled;
}
