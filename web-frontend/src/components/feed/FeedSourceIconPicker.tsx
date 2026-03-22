import React from 'react';
import { Tooltip } from 'antd';
import { CHANNEL_TYPES } from './feedChannelConfig';

/** Add Feed modal: icon picker for channel type */
const FeedSourceIconPicker: React.FC<{
  value?: string;
  onChange?: (v: string) => void;
  disabled?: boolean;
}> = ({ value, onChange, disabled }) => (
  <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
    {CHANNEL_TYPES.map((type) => {
      const selected = value === type.value;
      const isDisabled = type.disabled || disabled;
      return (
        <Tooltip key={type.value} title={type.disabled ? 'Coming Soon' : type.label}>
          <div
            role="button"
            tabIndex={0}
            onClick={() => !isDisabled && onChange?.(type.value)}
            onKeyDown={(e) => !isDisabled && (e.key === 'Enter' || e.key === ' ') && onChange?.(type.value)}
            style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              width: 80,
              height: 72,
              borderRadius: 12,
              border: `2px solid ${selected ? type.color : 'transparent'}`,
              backgroundColor: selected ? `${type.color}14` : undefined,
              color: type.color,
              cursor: isDisabled ? 'not-allowed' : 'pointer',
              opacity: isDisabled ? 0.5 : 1,
              transition: 'border-color .2s, background-color .2s',
            }}
          >
            <span style={{ fontSize: 28, marginBottom: 4 }}>{type.icon}</span>
            <span style={{ fontSize: 12, color: 'inherit', opacity: 0.9 }}>{type.label}</span>
          </div>
        </Tooltip>
      );
    })}
  </div>
);

export default FeedSourceIconPicker;
