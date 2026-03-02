# 在香港阿里云创建 OSS Bucket 指南

本文档结合项目代码对 OSS 的使用方式，说明如何在**香港地域**创建 Bucket，并与现有配置对接。

---

## 一、控制台创建 Bucket（关键配置）

在「创建 Bucket」页面请按以下方式填写（与你提供的截图对应）：

### 1. 地域（必改）

- **当前截图**：华北2 (北京) → **请改为：中国香港**
- 选择后，Endpoint 会显示为：`oss-cn-hongkong.aliyuncs.com`
- 创建后地域不可修改；香港非中国内地，不受「2025年3月20日后默认公网域名限制」影响，可直接用该 Endpoint 调用 API。

### 2. Bucket 名称

- 填写一个**全局唯一**的名称，例如：`rc-audio-files` 或 `yourcompany-rc-files-hk`（若 `rc-audio-files` 已被占用可换名）。
- 创建后不可修改；名称会用于项目配置中的 `OSS_BUCKET_NAME`。

### 3. 存储类型

- **标准存储**：与当前项目用法一致（音频、文档、图片经常访问），保持选择「标准存储」即可。

### 4. 存储冗余类型

- 同城冗余或本地冗余均可；按成本与可用性需求选择。

### 5. 版本控制

- 可选「开启 防误删」或「关闭」；代码不依赖版本控制，按需选择。

### 6. 阻止公共访问

- **建议保持「已开通」**。项目通过 **签名 URL（临时链接）** 访问文件，不需要匿名公共读，保持阻止公共访问更安全。

### 7. 读写权限

- **必须选「私有」**。  
  代码中 `backend/services/oss_service.py` 使用 `get_signed_url()` 生成临时访问链接，依赖私有桶 + 签名访问；选「公共读/公共读写」会与「阻止公共访问」冲突且不符合当前设计。

### 8. 其他

- **服务端加密**：可选「无」或按合规需求选择 OSS 托管/KMS。  
- **实时日志查询**：按需开启，不影响业务逻辑。  
- **所属资源组**：按账号规划选择。

创建完成后，记下：

- **Bucket 名称**（如 `rc-audio-files`）
- **地域**：中国香港 → Endpoint 为 **`oss-cn-hongkong.aliyuncs.com`**

---

## 二、项目配置如何对接

代码从环境变量读取 OSS 配置（见 `backend/services/oss_service.py` 和 `deployment/config.defaults.env`）：

| 环境变量 | 说明 | 香港 Bucket 建议值 |
|----------|------|--------------------|
| `OSS_BUCKET_NAME` | 刚创建的 Bucket 名称 | 与控制台一致，如 `rc-audio-files` |
| `OSS_ENDPOINT` | 必须与 Bucket 地域一致 | `oss-cn-hongkong.aliyuncs.com` |
| `OSS_ACCESS_KEY_ID` | AccessKey | 在 `.env.secrets` 中配置，与 Bucket 同账号 |
| `OSS_ACCESS_KEY_SECRET` | AccessKey 密钥 | 同上 |
| `OSS_AUDIO_PREFIX` | 音频对象前缀 | 保持 `audio/` |
| `OSS_DOCUMENT_PREFIX` | 文档对象前缀 | 保持 `documents/` |
| `OSS_IMAGE_PREFIX` | 图片对象前缀 | 保持 `images/` |
| `OSS_TEMP_PREFIX` | 临时对象前缀 | 保持 `temp/` |
| `OSS_CUSTOM_DOMAIN` | 可选，自定义域名 | 香港一般可不配；内地需备案域名时可再配 |

**重要**：  
- 若 Bucket 建在**香港**，`OSS_ENDPOINT` 必须为 **`oss-cn-hongkong.aliyuncs.com`**，不能再用深圳等其它地域的 endpoint。  
- 当前仓库默认是深圳：`OSS_ENDPOINT=oss-cn-shenzhen.aliyuncs.com`，改用香港 Bucket 后需改为上述香港 endpoint。

### 修改方式

1. **方式 A：改默认配置（推荐，便于部署脚本统一生成 .env）**  
   编辑 `deployment/config.defaults.env`：

   ```bash
   OSS_BUCKET_NAME=rc-audio-files
   OSS_ENDPOINT=oss-cn-hongkong.aliyuncs.com
   ```

2. **方式 B：仅在当前服务器覆盖**  
   在 `deployment/deploy.conf` 中增加或覆盖（若 deploy 脚本支持从 deploy.conf 输出到 .env）：

   ```ini
   OSS_BUCKET_NAME=rc-audio-files
   OSS_ENDPOINT=oss-cn-hongkong.aliyuncs.com
   ```

3. **AccessKey**  
   继续在 `deployment/.env.secrets` 中配置 `OSS_ACCESS_KEY_ID` 和 `OSS_ACCESS_KEY_SECRET`（需对该香港 Bucket 有读写权限）。

修改后执行：

```bash
sudo bash deployment/deploy.sh && sudo ./restart_services.sh
```

---

## 三、代码侧要点（无需改代码）

- **上传**：`upload_file` / `upload_audio_file` 会按前缀把文件写入 `audio/`、`documents/`、`images/`、`temp/`，与 Bucket 地域无关，只要 endpoint 正确即可。  
- **访问**：私有桶通过 `get_signed_url()` 生成临时 URL；若配置了 `OSS_CUSTOM_DOMAIN`，会优先用自定义域名。  
- **Worker**：`file_worker.py`、`file_tasks.py` 会从 OSS 下载到本地再处理，只要 `OSS_ENDPOINT` 与 Bucket 地域一致即可。

---

## 四、检查清单

创建香港 Bucket 并上线前建议确认：

- [ ] 地域已选「中国香港」，Endpoint 为 `oss-cn-hongkong.aliyuncs.com`
- [ ] Bucket 名已确定，且与 `OSS_BUCKET_NAME` 一致
- [ ] 读写权限为「私有」，阻止公共访问建议开启
- [ ] `OSS_ENDPOINT=oss-cn-hongkong.aliyuncs.com` 已写入 `config.defaults.env` 或 `deploy.conf`
- [ ] 已执行 `deploy.sh` 并重启服务
- [ ] 用控制台或接口上传一个测试文件，在应用内能正常预览/下载（签名 URL 有效）

按上述步骤即可在香港阿里云创建 OSS Bucket，并与当前项目正确对接。
