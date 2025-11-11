export {};

const isDomNotFoundError = (error: unknown): boolean => {
  if (!error || typeof error !== 'object') {
    return false;
  }

  const maybeDomException = error as {
    name?: string;
    code?: number;
  };

  if (maybeDomException.name === 'NotFoundError') {
    return true;
  }

  // Older browsers use numeric DOMException codes.
  return maybeDomException.code === 8;
};

(() => {
  if (typeof window === 'undefined' || typeof Node === 'undefined') {
    return;
  }

  const proto = Node.prototype as {
    removeChild(child: Node): Node;
    __removeChildPatched?: boolean;
  };

  if (proto.__removeChildPatched) {
    return;
  }

  const originalRemoveChild = proto.removeChild;

  if (typeof originalRemoveChild !== 'function') {
    return;
  }

  const isRcUtilStyleNode = (node: Node | null): node is HTMLStyleElement =>
    !!node &&
    node instanceof HTMLStyleElement &&
    typeof node.getAttribute === 'function' &&
    !!node.getAttribute('data-rc-order');

  const patchedRemoveChild: typeof originalRemoveChild = function patched(this: Node, child: Node) {
    try {
      return originalRemoveChild.call(this, child);
    } catch (error) {
      if (isDomNotFoundError(error)) {
        if (isRcUtilStyleNode(child)) {
          const actualParent = child.parentNode;
          if (actualParent && actualParent !== this) {
            return originalRemoveChild.call(actualParent, child);
          }

          if (!actualParent) {
            return child;
          }
        } else {
          const parentAfterFailure = child.parentNode;
          if (!parentAfterFailure || !document.contains(child)) {
            return child;
          }
        }

        return child;
      }

      throw error;
    }
  };

  Object.defineProperty(patchedRemoveChild, '__patched', {
    value: true,
  });

  proto.removeChild = patchedRemoveChild;
  Object.defineProperty(proto, '__removeChildPatched', {
    value: true,
  });
})();


