import React, { useEffect, useRef, useState } from 'react';
import { useAuthStore } from '../store/authStore';
import { useScrollReveal, useScrolled } from '../hooks/useScrollReveal';
import { GuanHeLogoCombo } from '../components/GuanHeLogoCombo';
import './LandingPage.css';

/* 首屏背景视频（参照 FengHe 官网做法） */
const MEDIA_BASE = `${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/media`;

/* 背景视频双源：手机/微信走 1080p 轻量版，桌面走 4K 原片。
   原片为 3440×1946 High@L5.2 yuvj420p（full-range）约 22.5Mbps——桌面浏览器解码无碍，
   但微信内核（X5/XWeb）与国产 ROM 硬解对 full-range/L5.2 兼容差，且 40MB 在跨境
   链路必然卡死。轻量版为 1920×1080 High@L4.0 limited-range 2-pass 4Mbps（6.9MB），
   手机全屏观感与原片无差。
   判定：移动/微信 UA，或触屏 + 短边 ≤1024（覆盖 iPad 全系）。 */
const IS_LITE_VIDEO =
  /Android|iPhone|iPad|iPod|Mobile|MicroMessenger/i.test(navigator.userAgent) ||
  (navigator.maxTouchPoints > 1 &&
    Math.min(window.screen.width, window.screen.height) <= 1024);
const HERO_VIDEO_URL = IS_LITE_VIDEO
  ? `${MEDIA_BASE}/hero-guanhe-mobile.mp4`
  : `${MEDIA_BASE}/hero-guanhe.mp4?v=20261114e`;

/* 微信视频诊断面板数据（?vdebug=1 开启；换 URL 天然绕开微信旧会话缓存） */
interface VdebugInfo {
  src: string;
  net: number;
  ready: number;
  t: string;
  paused: boolean;
  err: string;
}
/* 「观合」书法字标：Zhi Mang Xing 行书高清渲染图（与 FengHe「风和」同款笔意） */
const BRAND_BASE = `${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/brand`;

/* 登录弹窗阶段：null 关闭 / role 身份选择 / disclaimer 合规声明 / notice 投资人门户占位 */
type LoginModal = null | 'role' | 'disclaimer' | 'notice';

/* 免責/合規聲明（要素參考多家香港 9 號牌同業 + FengHe 口徑，獨立撰寫，繁體中文） */
const DISCLAIMER_PARAS = [
  '本網站由 GuanHe Capital 營運。GuanHe Capital 為香港資產管理公司，根據《證券及期貨條例》獲證券及期貨事務監察委員會發牌，可進行第9類（資產管理）受規管活動。本網站所載資料僅涉及私募基金，並只供《證券及期貨條例》（香港法例第571章）或其所屬司法管轄區同等制度下定義的專業投資者查閱。零售客戶及依法不得獲傳達該等資料之任何人士，不得查閱或依賴本網站內容。閣下繼續瀏覽，即表示確認並保證閣下符合上述資格。',
  '本網站內容僅供一般參考。其編製並無顧及任何個別人士或實體的投資目標、財務狀況或特定需要，本公司亦不會據此徵求閣下作出任何行動。本網站任何內容均不構成稅務、會計、監管、法律或投資意見；文內所表達的觀點僅代表 GuanHe Capital 於發布當時的立場，並可視情況變化隨時修訂，恕不另行通知。',
  '本網站任何部分均不構成、亦不應被視為 GuanHe Capital 或其關連公司作出的促銷、推薦、招攬、邀請或要約，以購買或出售任何證券、集體投資計劃或其他金融工具或服務；任何此等內容亦不得被視為向公眾發出的認購任何金融產品或其他交易的邀請或要約。凡在某司法管轄區的證券法例下，作出該等要約、招攬或促銷即屬違法——包括但不限於美國人士——本網站並非針對該轄區人士而設；如閣下受此限制，請立即離開本網站。閣下須自行了解並遵守適用於閣下的法律及監管規定。',
  '投資涉及風險，回報並無保證。基金投資的價值可升亦可跌，過往表現並非日後結果的保證，閣下取回的金額可能少於原來投入的本金。投資組合權益與存放於銀行的存款有根本差異，並不受同等保障。合資格投資者於投資前應詳閱相關基金的發售及披露文件，當中載列（其中包括）適用的風險因素、收費及開支。',
  'GuanHe Capital 相信本網站所載資料於發布時屬可靠，並已採取合理措施確保其與時並進，惟概不就其準確性、完整性或時效性作出任何保證，且該等資料可隨時修訂、補充或撤回，恕不另行通知。本網站及其內容乃「如實」提供，不論明示或暗示，概不就所載資料、文字、圖像或連結之正確性、任何功能之不間斷或無誤差運作、任何錯誤之修正、或無病毒及其他有害成分，作出任何陳述或保證。',
  '在法律允許的最大範圍內，GuanHe Capital 連同其關連公司、董事、高級人員、僱員及代理，概不就本網站資料中的錯誤或遺漏，或任何人士（包括第三方）對該等資料之使用、依賴或詮釋所引致的任何損失或損害承擔責任。',
  'GuanHe Capital 之唯一官方網站為 www.guanhecapital.hk。本公司與任何採用相近名稱、品牌或域名以推廣經紀、交易、投資或類似金融服務之其他網站或實體概無關連，亦從未授權該等網站或實體，且概不就任何該等無關連網站之內容、保安或服務承擔責任。',
  '未經 GuanHe Capital 事先書面批准，本網站任何部分均不得複製、傳送或分發（全部或部分）。本等條款以及閣下對本網站之使用，均受香港特別行政區法律管轄。',
];

const LandingPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const scrolled = useScrolled(40);
  const [modal, setModal] = useState<LoginModal>(null);
  const sloganRef = useRef<HTMLHeadingElement>(null);
  const taglineRef = useRef<HTMLParagraphElement>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const [vdebug, setVdebug] = useState<VdebugInfo | null>(null);

  // 背景视频：微信兜底 + 错误记录 + ?vdebug=1 诊断面板
  // （1）iOS 微信部分版本 autoplay 需 WeixinJSBridge ready 后触发；
  // （2）Android 老内核（X5）需 x5-video-player-type 启用 H5 同层播放（见 video 属性）；
  // （3）出错时静默降级为 poster（CSS 已兜底），console 留痕便于排查；
  // （4）返回导航兜底：从 /login 等路由 back 回来时组件重挂载，autoplay 已无
  //     活跃用户手势常被移动端拒绝——数据就绪、页面恢复可见、用户的任意一次
  //     触摸/点击（捕获阶段，全局）都会重试播放，self-healing。
  useEffect(() => {
    const v = videoRef.current;
    if (!v) return;

    const tryPlay = () => {
      v.play().catch(() => {});
    };
    const onWxReady = () => tryPlay();
    const wx = (window as unknown as { WeixinJSBridge?: { invoke: (a: string, b: object, cb: () => void) => void } }).WeixinJSBridge;
    if (typeof wx === 'undefined') {
      document.addEventListener('WeixinJSBridgeReady', onWxReady, false);
    } else {
      wx.invoke('getNetworkType', {}, () => tryPlay());
    }

    const onMediaReady = () => tryPlay();
    const onVisible = () => {
      if (document.visibilityState === 'visible') tryPlay();
    };
    const onGesture = () => {
      if (v.paused) tryPlay();
    };
    v.addEventListener('canplay', onMediaReady);
    v.addEventListener('loadeddata', onMediaReady);
    document.addEventListener('visibilitychange', onVisible);
    // passive：不阻塞滚动；capture：任何区域（含弹窗/按钮）的触摸都能救活视频
    document.addEventListener('touchend', onGesture, { capture: true, passive: true });
    document.addEventListener('click', onGesture, true);

    const onError = () => {
      const e = v.error;
      console.error('[hero] video error', e && { code: e.code, message: e.message });
    };
    v.addEventListener('error', onError);

    const isDebug = new URLSearchParams(window.location.search).has('vdebug');
    let timer: number | undefined;
    if (isDebug) {
      const tick = () =>
        setVdebug({
          src: v.currentSrc || v.src || '',
          net: v.networkState,
          ready: v.readyState,
          t: v.currentTime.toFixed(2),
          paused: v.paused,
          err: v.error ? `${v.error.code} ${v.error.message || ''}` : '-',
        });
      tick();
      timer = window.setInterval(tick, 500);
    }

    return () => {
      document.removeEventListener('WeixinJSBridgeReady', onWxReady);
      v.removeEventListener('canplay', onMediaReady);
      v.removeEventListener('loadeddata', onMediaReady);
      document.removeEventListener('visibilitychange', onVisible);
      document.removeEventListener('touchend', onGesture, true);
      document.removeEventListener('click', onGesture, true);
      v.removeEventListener('error', onError);
      if (timer) window.clearInterval(timer);
    };
  }, []);

  // Slogan：每次进入视口都重播字体入场效果（离开即复位）
  useEffect(() => {
    // 移动端浏览器滚动到底/回弹时会露出 html 背景（默认白），与深蓝页脚形成大片白边
    const html = document.documentElement;
    const prevBg = html.style.background;
    html.style.background = '#0a4a8c'; // 与 footer 底部同色
    return () => {
      html.style.background = prevBg;
    };
  }, []);

  useEffect(() => {
    const els = [sloganRef.current, taglineRef.current].filter(
      (el): el is NonNullable<typeof el> => el !== null
    );
    if (els.length === 0) return;
    if (!('IntersectionObserver' in window)) {
      els.forEach((el) => el.classList.add('is-in'));
      return;
    }
    const io = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          entry.target.classList.toggle('is-in', entry.isIntersecting);
        });
      },
      { threshold: 0.35 }
    );
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);

  useScrollReveal();

  // LOGIN：先弹身份选择（Staff / Investor）
  const goLogin = () => setModal('role');

  // 员工：另开独立浏览器窗口进入 GuanHe Terminal——Terminal 是独立系统，不属于官网。
  // 带尺寸/工具栏特性时浏览器会弹出全新窗口（而非当前窗口里的新标签页）
  const chooseStaff = () => {
    setModal(null);
    window.open(
      isAuthenticated ? '/feeds-dashboard' : '/login',
      '_blank',
      'width=1280,height=900,menubar,toolbar,location,status,resizable,scrollbars'
    );
  };

  // 弹窗打开时：锁定背景滚动 + Esc 关闭
  useEffect(() => {
    if (!modal) return;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setModal(null);
    };
    window.addEventListener('keydown', onKey);
    return () => {
      document.body.style.overflow = prevOverflow;
      window.removeEventListener('keydown', onKey);
    };
  }, [modal]);

  return (
    <div className="landing" id="top">
      {/* ---------- 顶部导航：居中 Logo（观合书法 + GuanHe Capital），LOGIN 靠视口最右 ---------- */}
      <header className={`fh-header${scrolled ? ' is-scrolled' : ''}`}>
        <div className="fh-header-inner">
          <a href="#top" className="fh-brand" aria-label="GuanHe Capital">
            {/* 观合 × GuanHe Capital. 组合字标：单体内联 SVG（见 components/GuanHeLogoCombo） */}
            <GuanHeLogoCombo />
          </a>
        </div>
        <button className="fh-login" onClick={goLogin}>
          Login
        </button>
      </header>

      {/* ---------- Hero：全屏视频背景 + 标语 ---------- */}
      <section className="fh-hero">
        <div className="fh-hero-bg" aria-hidden="true">
          <div className="fh-hero-glow fh-glow-a" />
          <div className="fh-hero-glow fh-glow-b" />
          <video
            className="fh-hero-video"
            ref={videoRef}
            autoPlay
            muted
            loop
            playsInline
            preload="auto"
            poster={`${MEDIA_BASE}/hero-poster.jpg?v=20261114e`}
            src={HERO_VIDEO_URL}
            // X5（微信安卓老内核）H5 同层播放，避免被系统播放器接管/黑屏；
            // 非标准属性，绕过 TS 的已知属性检查
            {...({
              'x5-video-player-type': 'h5',
              'x5-video-player-fullscreen': 'true',
            } as React.VideoHTMLAttributes<HTMLVideoElement>)}
          />
        </div>
        {vdebug && (
          <pre className="fh-vdebug">
            {`[src] ${vdebug.src}\n[net] ${vdebug.net}  [ready] ${vdebug.ready}  [t] ${vdebug.t}s  [paused] ${vdebug.paused}\n[err] ${vdebug.err}\n[ua] ${navigator.userAgent}`}
          </pre>
        )}
        <h1 className="fh-hero-title" ref={sloganRef}>
          <span className="fh-title-row">Shared values,</span>
          <span className="fh-title-row">
            <span className="fh-title-spacer" aria-hidden="true">
              Shared&nbsp;
            </span>
            joint efforts.
          </span>
        </h1>
      </section>

      {/* ---------- Overview：内容暂时留白，待真实数据后再上 ---------- */}
      <section className="fh-overview fh-overview-blank" aria-hidden="true" />

      {/* ---------- 品牌哲学：观合（竖排） ---------- */}
      <section className="fh-philosophy">
        <img
          className="fh-watermark fh-wm-guan"
          src={`${BRAND_BASE}/ghz-guan-white.png`}
          alt=""
          aria-hidden="true"
        />
        <img
          className="fh-watermark fh-wm-he"
          src={`${BRAND_BASE}/ghz-he-white.png`}
          alt=""
          aria-hidden="true"
        />
        <div className="fh-philosophy-inner reveal">
          {/* 品牌名横排：观 (Guan) 合 (He) —— 参照 FengHe 页脚 风(Feng)和(He) 排版 */}
          <div className="fh-philosophy-name">
            <span className="fh-name-pair">
              <span className="fh-name-char">观</span>
              <span className="fh-name-roman">(Guan)</span>
            </span>
            <span className="fh-name-pair">
              <span className="fh-name-char">合</span>
              <span className="fh-name-roman">(He)</span>
            </span>
          </div>
          <p className="fh-tagline" ref={taglineRef}>
            <span className="fh-tagline-row">Do the right things.</span>
            <span className="fh-tagline-row">Do things right.</span>
          </p>
        </div>
      </section>

      {/* ---------- 底部联系 ---------- */}
      <footer className="fh-footer">
        <div className="fh-footer-inner">
          <div className="fh-footer-enquiry">
            <span className="fh-label">General Enquiries</span>
            <a className="fh-footer-mail" href="mailto:info@guanhecapital.hk">
              info@guanhecapital.hk
            </a>
          </div>
          <div className="fh-footer-copy">© 2026. GuanHe Capital. All rights reserved.</div>
        </div>
      </footer>

      {/* ---------- 登录弹窗组：身份选择 / 合规声明 / 投资人门户占位 ---------- */}
      {modal && (
        <div
          className="fh-modal-mask"
          onClick={() => setModal(null)}
          role="dialog"
          aria-modal="true"
        >
          <div className="fh-modal" onClick={(e) => e.stopPropagation()}>

            {modal === 'role' && (
              <>
                <button className="fh-modal-x" onClick={() => setModal(null)} aria-label="Close">
                  ×
                </button>
                <div className="fh-modal-kicker">GuanHe Capital</div>
                <h3 className="fh-modal-title">Login</h3>
                <p className="fh-modal-sub">Please identify yourself to continue.</p>
                <div className="fh-role-grid">
                  <button className="fh-role-card" onClick={chooseStaff}>
                    <span className="fh-role-name">Staff</span>
                  </button>
                  <button className="fh-role-card" onClick={() => setModal('disclaimer')}>
                    <span className="fh-role-name">Investor</span>
                  </button>
                </div>
              </>
            )}

            {modal === 'disclaimer' && (
              <>
                <button className="fh-modal-x" onClick={() => setModal(null)} aria-label="Close">
                  ×
                </button>
                <div className="fh-modal-kicker">GuanHe Capital</div>
                <div className="fh-modal-body">
                  {DISCLAIMER_PARAS.map((p, i) => (
                    <p key={i}>{p}</p>
                  ))}
                </div>
                <div className="fh-modal-actions">
                  <button
                    className="fh-modal-btn fh-modal-btn-ghost"
                    onClick={() => setModal(null)}
                  >
                    I Do Not Accept
                  </button>
                  <button
                    className="fh-modal-btn fh-modal-btn-primary"
                    onClick={() => setModal('notice')}
                  >
                    I Accept
                  </button>
                </div>
              </>
            )}

            {modal === 'notice' && (
              <>
                <button className="fh-modal-x" onClick={() => setModal(null)} aria-label="Close">
                  ×
                </button>
                <div className="fh-modal-kicker">Investor Portal</div>
                <h3 className="fh-modal-title">Coming Soon</h3>
                <p className="fh-modal-sub">
                  The investor login is currently under development and will be available
                  shortly. For fund information and reporting enquiries in the meantime,
                  please contact{' '}
                  <a className="fh-notice-mail" href="mailto:info@guanhecapital.hk">
                    info@guanhecapital.hk
                    </a>
                  .
                </p>
                <div className="fh-modal-actions">
                  <button
                    className="fh-modal-btn fh-modal-btn-primary"
                    onClick={() => setModal(null)}
                  >
                    Back
                  </button>
                </div>
              </>
            )}

          </div>
        </div>
      )}
    </div>
  );
};

export default LandingPage;
