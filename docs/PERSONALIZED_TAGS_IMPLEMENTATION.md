# 用户个性化标签系统实现文档

## 📋 概述

实现了用户个性化标签系统，解决了原有标签系统的核心问题：**标签现在是"用户对主题的分类"，而不是"主题的属性"**。

### 🎯 解决的问题

**问题1：标签全局可见**
- 原来：任何人给主题打 `Market Weekly` 标签，所有用户都会在 Market Weekly 报告中看到
- 现在：每个用户的标签是独立的，只影响自己的报告

**问题2：无法使用别人分享的主题**
- 原来：用户B无法给用户A分享的主题打标签（因为权限限制）
- 现在：用户可以给任何可见的主题（自己创建的 + 已分享的）打标签

---

## 🔄 核心变更

### 1. 数据库架构更改

**迁移文件：** `backend/database/migrations/add_user_id_to_theme_tags.sql`

**主要变更：**
- 在 `theme_tags` 表中添加 `user_id` 字段（NOT NULL）
- 修改唯一约束：从 `UNIQUE(theme_id, tag_name)` 改为 `UNIQUE(theme_id, tag_name, user_id)`
- 创建索引：`idx_theme_tags_user_id`, `idx_theme_tags_user_tag`
- 数据迁移：将现有标签分配给主题创建者

**表结构：**
```sql
theme_tags (
    id SERIAL PRIMARY KEY,
    theme_id VARCHAR(255) NOT NULL,
    tag_name VARCHAR(100) NOT NULL,
    user_id VARCHAR(100) NOT NULL,  -- 🔥 新增
    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    UNIQUE(theme_id, tag_name, user_id)  -- 🔥 修改
)
```

**语义说明：**
- `user_id`: 记录是"谁"给这个主题打的标签
- 同一个主题可以被不同用户独立打上相同的标签
- 例如：Admin 和 User B 都可以给 Theme A 打 `Market Weekly` 标签，互不影响

---

### 2. 后端 API 更新

#### 2.1 批量标签操作 (`/api/investment-themes/batch-tag`)

**文件：** `backend/api/investment_themes.py`

**主要变更：**
1. **权限检查优化**
   - 允许用户给可见的主题打标签（自己创建的 + 已分享的）
   - 用户只能删除自己的标签

2. **添加标签时记录 user_id**
   ```python
   INSERT INTO theme_tags (theme_id, tag_name, user_id, created_by, created_at)
   VALUES (%s, %s, %s, %s, NOW())
   ON CONFLICT (theme_id, tag_name, user_id) DO NOTHING
   ```

3. **删除标签时只删除当前用户的标签**
   ```python
   DELETE FROM theme_tags
   WHERE theme_id IN (...)
     AND tag_name IN (...)
     AND user_id = %s  -- 🔥 只删除当前用户的标签
   ```

#### 2.2 主题列表查询 (`/api/investment-themes/themes`)

**主要变更：**
1. **查询标签时筛选当前用户**
   ```python
   SELECT theme_id, ARRAY_AGG(tag_name ORDER BY created_at) AS tags
   FROM theme_tags
   WHERE theme_id = ANY(%s)
     AND user_id = %s  -- 🔥 只返回当前用户的标签
   GROUP BY theme_id
   ```

2. **标签筛选逻辑更新**
   ```python
   # 筛选拥有指定标签的主题（当前用户的标签）
   EXISTS (
       SELECT 1 FROM theme_tags tt
       WHERE tt.theme_id = ite.theme_id
         AND tt.user_id = %s  -- 🔥 当前用户
         AND tt.tag_name IN (...)
   )
   ```

#### 2.3 可用标签查询 (`/api/investment-themes/available-tags`)

**主要变更：**
- 只返回当前用户的标签及其使用次数
  ```python
  SELECT tag_name, COUNT(DISTINCT theme_id) as count
  FROM theme_tags
  WHERE user_id = %s  -- 🔥 只查询当前用户的标签
  GROUP BY tag_name
  ORDER BY count DESC, tag_name
  ```

---

### 3. Market Weekly 服务更新

**文件：** `backend/services/market_weekly_service.py`

**主要变更：**

1. **`generate_report` 方法**
   - 添加 `user_id` 参数
   - 缓存key包含用户ID：`market_weekly_report:{period}:{user_id}`
   - 确保每个用户的报告独立缓存

2. **`_calculate_ai_related_performance` 方法**
   - 添加 `user_id` 参数
   - 查询时筛选用户个性化的 `Market Weekly` 标签
   ```python
   WHERE tt.tag_name = 'Market Weekly'
     AND tt.user_id = %s  -- 🔥 只查询当前用户的标签
     AND tca.is_verified = TRUE
   ```

3. **`_get_my_themes_stocks` 方法**
   - 添加 `user_id` 参数
   - 查询逻辑同上

**API调用更新：** `backend/api/market_weekly.py`
```python
report_data = service.generate_report(
    period=period, 
    use_cache=use_cache, 
    user_id=current_user.username  -- 🔥 传递用户ID
)
```

---

### 4. 前端 UI 更新

**文件：** `web-frontend/src/pages/InvestmentThemesPage.tsx`

**主要变更：**

1. **UI 文案更新**
   - 将 "Tags" 改为 "My Tags"
   - 添加说明文字：🏷️ "Personal tags for organizing your themes. These tags are private to you - other users cannot see them."

2. **Create New Theme 对话框**
   ```typescript
   <Form.Item 
     label="My Tags" 
     help="🏷️ Personal tags for organizing your themes (e.g., Market Weekly, Broker Report). These tags are private to you - other users cannot see them. You can tag any theme, including shared ones."
   >
   ```

3. **Edit Theme 对话框**
   ```typescript
   <Form.Item 
     label="My Tags" 
     help="🏷️ Your personal tags for this theme. These tags are only visible to you and help organize themes for reports like Market Weekly."
   >
   ```

4. **Batch Upload 对话框**
   ```typescript
   <Text type="secondary">
     🏷️ All themes in this batch will be tagged with your personal tags. These are only visible to you.
   </Text>
   ```

---

## 📊 使用场景示例

### 场景 1：Admin 创建并标记主题

```
1. Admin 创建 Theme A: "AI Chips"
2. Admin 给 Theme A 打标签: "Market Weekly"
3. 结果：
   - Admin 的 Market Weekly 报告中会显示 Theme A
   - 其他用户的 Market Weekly 报告中不会显示 Theme A（除非他们也打了标签）
```

### 场景 2：用户使用分享的主题

```
1. Admin 创建 Theme A 并分享出去
2. Admin 给 Theme A 打标签: "Market Weekly", "Broker Report"
3. User B 看到分享的 Theme A
4. User B 也给 Theme A 打标签: "Market Weekly"
5. 结果：
   - Admin 的 Market Weekly 报告中：显示 Theme A
   - User B 的 Market Weekly 报告中：也显示 Theme A
   - User C 的 Market Weekly 报告中：不显示 Theme A（User C 没有打标签）
```

### 场景 3：不同用户的独立标签

```
1. Admin 创建 Theme A 并分享
2. Admin 给 Theme A 打标签: "Market Weekly"
3. User B 给 Theme A 打标签: "Broker Report"
4. 结果：
   - Admin 的 Market Weekly 报告中：显示 Theme A
   - Admin 的 Broker Report 报告中：不显示 Theme A
   - User B 的 Market Weekly 报告中：不显示 Theme A
   - User B 的 Broker Report 报告中：显示 Theme A
```

---

## 🗄️ 数据库迁移步骤

### 执行迁移

```bash
# 1. 连接到数据库
psql -U your_user -d your_database

# 2. 执行迁移脚本
\i backend/database/migrations/add_user_id_to_theme_tags.sql

# 3. 验证迁移
SELECT COUNT(*) FROM theme_tags WHERE user_id IS NULL;  -- 应该返回 0

# 4. 查看用户标签分布
SELECT user_id, tag_name, COUNT(*) as theme_count
FROM theme_tags
GROUP BY user_id, tag_name
ORDER BY user_id, theme_count DESC;
```

### 回滚方案（如果需要）

```sql
-- 1. 备份数据
CREATE TABLE theme_tags_backup AS SELECT * FROM theme_tags;

-- 2. 删除新增的列和约束
ALTER TABLE theme_tags DROP CONSTRAINT theme_tags_theme_id_tag_name_user_id_key;
ALTER TABLE theme_tags DROP COLUMN user_id;

-- 3. 恢复旧的唯一约束
ALTER TABLE theme_tags ADD CONSTRAINT theme_tags_theme_id_tag_name_key UNIQUE(theme_id, tag_name);
```

---

## 🧪 测试场景

### 测试用例 1：创建和查看个性化标签

```
1. 登录为 User A
2. 创建 Theme X，打标签 "Market Weekly"
3. 登录为 User B
4. 查看主题列表 → Theme X 没有标签（因为是 User A 的标签）
5. User B 给 Theme X 打标签 "Market Weekly"
6. 查看主题列表 → Theme X 有标签 "Market Weekly"（User B 的标签）
7. User A 和 User B 的 Market Weekly 报告中都显示 Theme X
```

### 测试用例 2：标签筛选

```
1. 登录为 User A
2. 给 Theme 1 打标签 "Market Weekly"
3. 给 Theme 2 打标签 "Broker Report"
4. 在主题列表中按 "Market Weekly" 筛选 → 只显示 Theme 1
5. 在主题列表中按 "Broker Report" 筛选 → 只显示 Theme 2
```

### 测试用例 3：Market Weekly 报告

```
1. 登录为 User A
2. 给多个主题打标签 "Market Weekly"
3. 访问 Market Weekly 报告
4. 验证：只显示 User A 标记的主题
5. 登录为 User B
6. 访问 Market Weekly 报告
7. 验证：只显示 User B 标记的主题（可能为空）
```

---

## 🔍 API 响应示例

### 查询主题列表（包含标签）

**请求：**
```
GET /api/investment-themes/themes
Authorization: Bearer <user_token>
```

**响应：**
```json
{
  "success": true,
  "themes": [
    {
      "theme_id": "AI_Chips_001",
      "theme_name": "AI Chips",
      "category": "Semiconductors",
      "created_by": "admin",
      "is_shared": true,
      "tags": ["Market Weekly", "Broker Report"],  // 当前用户的标签
      "can_edit": false,  // 不是创建者，不能编辑
      "can_delete": false,
      "can_share": false
    }
  ],
  "total": 1
}
```

### 查询可用标签

**请求：**
```
GET /api/investment-themes/available-tags
Authorization: Bearer <user_token>
```

**响应：**
```json
{
  "success": true,
  "tags": [
    {"tag_name": "Market Weekly", "count": 15},  // 当前用户使用了15次
    {"tag_name": "Broker Report", "count": 3}
  ]
}
```

---

## 📝 注意事项

1. **缓存失效**
   - Market Weekly 报告缓存现在包含用户ID
   - 不同用户的报告不会相互污染

2. **向后兼容性**
   - 迁移脚本会自动将现有标签分配给主题创建者
   - 旧的 `group` 字段保留（但不再使用）

3. **性能优化**
   - 添加了复合索引 `idx_theme_tags_user_tag` 优化查询
   - 标签查询使用 `ARRAY_AGG` 减少数据库往返

4. **权限控制**
   - 用户可以给可见的主题打标签（自己创建的 + 已分享的）
   - 用户只能删除自己的标签
   - 标签操作不影响其他用户

---

## 🎯 优势总结

✅ **用户独立性**：每个用户的标签互不影响  
✅ **灵活性**：可以给任何可见的主题打标签  
✅ **隐私性**：标签是私有的，其他用户看不到  
✅ **可扩展性**：支持未来添加"公共标签"功能  
✅ **性能**：通过索引和缓存优化，保持高性能  

---

## 📚 相关文档

- [Theme Permissions and Sharing](./THEME_PERMISSIONS_AND_SHARING.md)
- [Theme Group Field Usage](./THEME_GROUP_FIELD_USAGE.md)
- [Theme Multi Tags Implementation](./THEME_MULTI_TAGS_IMPLEMENTATION.md)

---

**实施日期：** 2025-11-22  
**版本：** v2.0.0 (Personalized Tags)


