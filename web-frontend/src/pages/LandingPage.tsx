import React, { useEffect, useRef, useState } from 'react';
import { useAuthStore } from '../store/authStore';
import { useScrollReveal, useScrolled } from '../hooks/useScrollReveal';
import { GuanHeLogoCombo } from '../components/GuanHeLogoCombo';
import './LandingPage.css';

/* 首屏背景视频（参照 FengHe 官网做法） */
const MEDIA_BASE = `${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/media`;

/* 背景视频三态自适应（2026-11-16 大陆跨境链路优化）：
   - 高清版 hero-guanhe.mp4：1920×1080 High@L4.0 2-pass 2.5Mbps（≈4.4MB），桌面默认；
     （母带 3440×1946 @22.5Mbps 的 4K 原片 39.5MB 已弃用——桌面白字压画背景下肉眼无差，
      大陆无 VPN 用户却要为此硬啃 40MB；母带保留在 assets-src 可随时重出）
   - 轻量版 hero-guanhe-mobile.mp4：1280×720 2-pass 0.95Mbps（1.66MB）；
     手机/微信/平板一律轻量版（服务器在阿里云香港，大陆跨境直连实测吞吐仅 0.4~2Mbps，
     iOS 对码率高于链路吞吐的媒体会长期缓冲不出首帧）。
   桌面恒默认高清，唯一的降级通道是「运行时看门狗」（全浏览器，见下方 useEffect）：
   采样 video.buffered 增长速率，下载持续跟不上播放（4 秒采样窗缓冲增长 <1.2 秒内容
   ≈ 下载速率不足码率 30%，物理上永远缓冲不出来）→ 热切换轻量版。
   教训：不用 Network Information API 做初始分档——Chrome 的 downlink 估计值
   在 VPN 隧道下会长期偏低（RTT 被握手开销抬高），快网被误降轻量版（实际复现过）；
   基于真实缓冲速率的看门狗才是证据驱动的降级。
   判定移动端：移动/微信 UA，或触屏 + 短边 ≤1024 + 主输入为手指（pointer: coarse，
   覆盖 iPad 全系；触屏笔记本主输入是鼠标/触控板，不在此列）。 */
const IS_MOBILE_LIKE =
  /Android|iPhone|iPad|iPod|Mobile|MicroMessenger/i.test(navigator.userAgent) ||
  (navigator.maxTouchPoints > 1 &&
    Math.min(window.screen.width, window.screen.height) <= 1024 &&
    window.matchMedia?.('(pointer: coarse)').matches === true);
const HERO_URL_HD = `${MEDIA_BASE}/hero-guanhe.mp4?v=20261116c`;
const HERO_URL_LITE = `${MEDIA_BASE}/hero-guanhe-mobile.mp4?v=20261116b`;
type VideoTier = 'hd' | 'lite';

/* 安卓微信（XWeb）：开局直接进动画 WebP 模式，零等待自动动。
   新版 XWeb 对无手势视频硬性拦截（play 重试/WeixinJSBridge 均无效，实测），
   与其「静态海报数秒 → 再切动画」，不如开局即动画；顺带省掉视频下载字节
   （微信内动画图已足够，慢链路上反而更稳）。iOS 微信不受影响（真视频自动
   播正常），其余环境保留「视频优先 + 硬拒绝 6~12 秒后切动画」的兜底路径 */
const IS_ANDROID_WECHAT =
  /MicroMessenger/i.test(navigator.userAgent) && /Android/i.test(navigator.userAgent);

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
  // 视频档位：移动端恒轻量，桌面恒默认高清——降级只由运行时看门狗基于
  // 真实缓冲速率触发（见下方 useEffect），不用网络信息 API 做初始猜测
  const [videoTier, setVideoTier] = useState<VideoTier>(
    IS_MOBILE_LIKE ? 'lite' : 'hd'
  );
  // 自动播放被「硬拒绝」（WebView 策略，重试无效）时切换为循环动画 WebP：
  // <img> 不受媒体自动播放策略限制，保住零点击自动动。
  // 安卓微信开局即 true（零等待，见 IS_ANDROID_WECHAT 注释）
  const [videoBlocked, setVideoBlocked] = useState<boolean>(IS_ANDROID_WECHAT);

  // 浏览器标签页标题：官网固定「Home | GuanHe Capital」。index.html 的静态标题
  // 即此值（首载即本页）；从终端路由返回官网时由这里把标题改回来
  useEffect(() => {
    document.title = 'Home | GuanHe Capital';
  }, []);

  // 桌面高清版看门狗：按 1080p/2.5Mbps（≈0.31MB/s）校准阈值。
  // 每 4 秒采样一次 buffered 末端的增长量——增长 <1.2 秒内容（下载速率不足码率
  // 30%，物理上永远追不平）即判链路带不动，热切轻量版；
  // 健康退出：已开播且缓冲领先播放位置 3 秒。快网（含 Low Power Mode 短暂
  // 不预载的场景）会在几秒内填满缓冲正常退出，不会误降级。
  useEffect(() => {
    if (videoTier !== 'hd') return;
    const v = videoRef.current;
    if (!v) return;
    const bufEnd = () => (v.buffered.length ? v.buffered.end(v.buffered.length - 1) : 0);
    const started = performance.now();
    let last = { t: started, buf: bufEnd() };
    const timer = window.setInterval(() => {
      const now = performance.now();
      const buf = bufEnd();
      if (!v.paused && v.readyState >= 3 && buf - v.currentTime > 3) {
        window.clearInterval(timer);
        return;
      }
      const windowed = (now - last.t) / 1000;
      const grew = buf - last.buf;
      if (windowed >= 4) {
        if ((now - started) / 1000 >= 5 && grew < 1.2) {
          setVideoTier('lite');
          window.clearInterval(timer);
          return;
        }
        last = { t: now, buf };
      }
      if ((now - started) / 1000 > 15 && buf < 2) {
        setVideoTier('lite');
        window.clearInterval(timer);
      }
    }, 1000);
    return () => window.clearInterval(timer);
  }, [videoTier]);

  // 档位热切换：React 只改 src 属性不触发重载，需手动 load()；
  // 元素上的 canplay/loadeddata/手势监听仍然有效，会自动接续重试播放
  const prevTierRef = useRef<VideoTier>(videoTier);
  useEffect(() => {
    if (prevTierRef.current === videoTier) return; // 首次挂载不重复 load()
    prevTierRef.current = videoTier;
    const v = videoRef.current;
    if (!v) return;
    v.load();
    v.play().catch(() => {});
  }, [videoTier]);

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

    // 静音双保险：React 的 muted 只作为 DOM property 写入、不落为 HTML 属性，
    // 个别 iOS Safari 版本在元素插入时评估自动播放策略会拿不到该状态，
    // 导致 muted 自动播放被拒（页面加 ref 强制兜底，幂等无害）
    v.muted = true;
    v.defaultMuted = true;
    v.setAttribute('muted', '');
    v.setAttribute('playsinline', '');

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

    // （5）自动播放有界重试（2026-11-16 小米微信复现：ready=4 数据全缓冲、
    //     paused=true、err=-——XWeb 对无手势 play 的策略拒绝，且部分厂商
    //     WebView 的自动播放白名单是页面加载后才异步就绪的，早期单次调用
    //     被拒后无人重试就永久卡住）。
    //     挂载后 12 秒内每 1.5 秒重试：仅在 paused 时尝试，播起来即停；
    //     期间若 WeixinJSBridge 是晚注入的，也补一次桥内播放（桥上下文的
    //     play 在微信内不受手势限制）。
    //     连续 4 次 NotAllowedError（≈6 秒）判定「硬拒绝」（如新版 XWeb 不再
    //     暴露桥、或用户开了省流/禁止自动播放）→ 置 videoBlocked，
    //     切换为循环动画 WebP（<img> 不受媒体自动播放策略限制，保住零点击自动动）。
    const retryStart = performance.now();
    let bridgeTried = false;
    let notAllowedCount = 0;
    const autoRetry = window.setInterval(() => {
      if (!v.paused) {
        notAllowedCount = 0;
        window.clearInterval(autoRetry);
        return;
      }
      if ((performance.now() - retryStart) / 1000 > 12) {
        window.clearInterval(autoRetry);
        // 窗口耗尽仍没播起来且数据已就绪 → 无论拒绝错误是否带标准名
        // （XWeb 可能抛无名错误，NotAllowedError 计数漏判），一律判硬拒绝切 WebP
        if (v.paused && v.readyState >= 3) {
          setVideoBlocked(true);
        }
        return;
      }
      v.play().catch((e: unknown) => {
        if ((e as { name?: string } | undefined)?.name === 'NotAllowedError') {
          notAllowedCount += 1;
        }
      });
      if (notAllowedCount >= 4) {
        setVideoBlocked(true);
        window.clearInterval(autoRetry);
        return;
      }
      const wxGlobal = (
        window as unknown as { WeixinJSBridge?: { invoke: (a: string, b: object, cb: () => void) => void } }
      ).WeixinJSBridge;
      if (!bridgeTried && typeof wxGlobal !== 'undefined') {
        bridgeTried = true;
        wxGlobal.invoke('getNetworkType', {}, () => tryPlay());
      }
    }, 1500);

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
      window.clearInterval(autoRetry);
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
      isAuthenticated ? '/feeds' : '/login',
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
        {/* 海报兜底层：内联注入背景图，视频自动播放被拒/弱网卡加载时帆船图必显
            （iOS Safari 的 poster 属性在该场景下可能整幅不绘制） */}
        <div
          className="fh-hero-bg"
          aria-hidden="true"
          style={{ backgroundImage: `url(${MEDIA_BASE}/hero-poster.jpg?v=20261116b)` }}
        >
          <div className="fh-hero-glow fh-glow-a" />
          <div className="fh-hero-glow fh-glow-b" />
          {videoBlocked ? (
            /* 动画 WebP 兜底：<img> 不受媒体自动播放策略限制（微信硬拒绝时保住
               「自动动起来」）；与视频同 class 复用铺满裁切样式 */
            <img
              className="fh-hero-video"
              src={`${MEDIA_BASE}/hero-guanhe-ambience.webp?v=20261116`}
              alt=""
              aria-hidden="true"
            />
          ) : (
            <video
              className="fh-hero-video"
              ref={videoRef}
              autoPlay
              muted
              loop
              playsInline
              preload="auto"
              poster={`${MEDIA_BASE}/hero-poster.jpg?v=20261116b`}
            src={videoTier === 'hd' ? HERO_URL_HD : HERO_URL_LITE}
            // X5（微信安卓老内核）H5 同层播放，避免被系统播放器接管/黑屏；
            // webkit-playsinline/x5-playsinline：老 WebView 的内联播放前缀属性；
            // 均为非标准属性，绕过 TS 的已知属性检查
            {...({
              'x5-video-player-type': 'h5',
              'x5-video-player-fullscreen': 'true',
              'x5-playsinline': 'true',
              'webkit-playsinline': 'true',
              'x-webkit-airplay': 'allow',
            } as React.VideoHTMLAttributes<HTMLVideoElement>)}
          />
          )}
        </div>
        {vdebug && (
          <pre className="fh-vdebug">
            {`[src] ${videoBlocked ? 'webp-fallback' : vdebug.src}\n[net] ${vdebug.net}  [ready] ${vdebug.ready}  [t] ${vdebug.t}s  [paused] ${vdebug.paused}\n[err] ${vdebug.err}\n[ua] ${navigator.userAgent}`}
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
          {/* 品牌名竖排：观（上）/ 合（下），无拼音、静态常显（动效按需求移除）——
              志莽行书（GuanHeZhiMang，OFL 免费商用）。该字体仅含简体「观」（无繁体「觀」），
              故用简体；见 LandingPage.css @font-face */}
          <div className="fh-philosophy-name">
            <span className="fh-name-char">观</span>
            <span className="fh-name-char">合</span>
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
