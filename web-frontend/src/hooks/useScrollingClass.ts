import { useEffect, useRef, type DependencyList } from 'react';

const SCROLL_IDLE_MS = 700;

function bindScrollingClass(el: HTMLElement) {
  let idleTimer: number | undefined;

  const onScroll = () => {
    el.classList.add('is-scrolling');
    if (idleTimer !== undefined) {
      window.clearTimeout(idleTimer);
    }
    idleTimer = window.setTimeout(() => {
      el.classList.remove('is-scrolling');
    }, SCROLL_IDLE_MS);
  };

  el.addEventListener('scroll', onScroll, { passive: true });
  return () => {
    el.removeEventListener('scroll', onScroll);
    if (idleTimer !== undefined) {
      window.clearTimeout(idleTimer);
    }
    el.classList.remove('is-scrolling');
  };
}

/**
 * 滚动时临时加上 is-scrolling，配合 subtle-scrollbar 样式在滚动结束后隐藏滑块。
 */
export function useScrollingClass<T extends HTMLElement>() {
  const ref = useRef<T>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    return bindScrollingClass(el);
  }, []);

  return ref;
}

/** Tab 切换等场景：在 deps 变化时重新查找 DOM 并绑定滚动监听 */
export function useScrollingClassOnElement(
  getElement: () => HTMLElement | null,
  deps: DependencyList
) {
  useEffect(() => {
    const el = getElement();
    if (!el) return;
    return bindScrollingClass(el);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- getElement 由调用方按 deps 封装
  }, deps);
}
