import React from 'react';
import { Space } from 'antd';

interface CountryFlagProps {
  countryCode: string;
  showName?: boolean;
}

// 国家代码到国旗emoji的映射
const COUNTRY_FLAGS: { [key: string]: string } = {
  'AF': '🇦🇫', 'AL': '🇦🇱', 'DZ': '🇩🇿', 'AS': '🇦🇸', 'AD': '🇦🇩', 'AO': '🇦🇴', 'AI': '🇦🇮', 'AQ': '🇦🇶',
  'AG': '🇦🇬', 'AR': '🇦🇷', 'AM': '🇦🇲', 'AW': '🇦🇼', 'AU': '🇦🇺', 'AT': '🇦🇹', 'AZ': '🇦🇿', 'BS': '🇧🇸',
  'BH': '🇧🇭', 'BD': '🇧🇩', 'BB': '🇧🇧', 'BY': '🇧🇾', 'BE': '🇧🇪', 'BZ': '🇧🇿', 'BJ': '🇧🇯', 'BM': '🇧🇲',
  'BT': '🇧🇹', 'BO': '🇧🇴', 'BA': '🇧🇦', 'BW': '🇧🇼', 'BV': '🇧🇻', 'BR': '🇧🇷', 'IO': '🇮🇴', 'BN': '🇧🇳',
  'BG': '🇧🇬', 'BF': '🇧🇫', 'BI': '🇧🇮', 'KH': '🇰🇭', 'CM': '🇨🇲', 'CA': '🇨🇦', 'CV': '🇨🇻', 'KY': '🇰🇾',
  'CF': '🇨🇫', 'TD': '🇹🇩', 'CL': '🇨🇱', 'CN': '🇨🇳', 'CX': '🇨🇽', 'CC': '🇨🇨', 'CO': '🇨🇴', 'KM': '🇰🇲',
  'CG': '🇨🇬', 'CD': '🇨🇩', 'CK': '🇨🇰', 'CR': '🇨🇷', 'CI': '🇨🇮', 'HR': '🇭🇷', 'CU': '🇨🇺', 'CY': '🇨🇾',
  'CZ': '🇨🇿', 'DK': '🇩🇰', 'DJ': '🇩🇯', 'DM': '🇩🇲', 'DO': '🇩🇴', 'EC': '🇪🇨', 'EG': '🇪🇬', 'SV': '🇸🇻',
  'GQ': '🇬🇶', 'ER': '🇪🇷', 'EE': '🇪🇪', 'ET': '🇪🇹', 'FK': '🇫🇰', 'FO': '🇫🇴', 'FJ': '🇫🇯', 'FI': '🇫🇮',
  'FR': '🇫🇷', 'GF': '🇬🇫', 'PF': '🇵🇫', 'TF': '🇹🇫', 'GA': '🇬🇦', 'GM': '🇬🇲', 'GE': '🇬🇪', 'DE': '🇩🇪',
  'GH': '🇬🇭', 'GI': '🇬🇮', 'GR': '🇬🇷', 'GL': '🇬🇱', 'GD': '🇬🇩', 'GP': '🇬🇵', 'GU': '🇬🇺', 'GT': '🇬🇹',
  'GG': '🇬🇬', 'GN': '🇬🇳', 'GW': '🇬🇼', 'GY': '🇬🇾', 'HT': '🇭🇹', 'HM': '🇭🇲', 'VA': '🇻🇦', 'HN': '🇭🇳',
  'HK': '🇭🇰', 'HU': '🇭🇺', 'IS': '🇮🇸', 'IN': '🇮🇳', 'ID': '🇮🇩', 'IR': '🇮🇷', 'IQ': '🇮🇶', 'IE': '🇮🇪',
  'IM': '🇮🇲', 'IL': '🇮🇱', 'IT': '🇮🇹', 'JM': '🇯🇲', 'JP': '🇯🇵', 'JE': '🇯🇪', 'JO': '🇯🇴', 'KZ': '🇰🇿',
  'KE': '🇰🇪', 'KI': '🇰🇮', 'KP': '🇰🇵', 'KR': '🇰🇷', 'KW': '🇰🇼', 'KG': '🇰🇬', 'LA': '🇱🇦', 'LV': '🇱🇻',
  'LB': '🇱🇧', 'LS': '🇱🇸', 'LR': '🇱🇷', 'LY': '🇱🇾', 'LI': '🇱🇮', 'LT': '🇱🇹', 'LU': '🇱🇺', 'MO': '🇲🇴',
  'MK': '🇲🇰', 'MG': '🇲🇬', 'MW': '🇲🇼', 'MY': '🇲🇾', 'MV': '🇲🇻', 'ML': '🇲🇱', 'MT': '🇲🇹', 'MH': '🇲🇭',
  'MQ': '🇲🇶', 'MR': '🇲🇷', 'MU': '🇲🇺', 'YT': '🇾🇹', 'MX': '🇲🇽', 'FM': '🇫🇲', 'MD': '🇲🇩', 'MC': '🇲🇨',
  'MN': '🇲🇳', 'ME': '🇲🇪', 'MS': '🇲🇸', 'MA': '🇲🇦', 'MZ': '🇲🇿', 'MM': '🇲🇲', 'NA': '🇳🇦', 'NR': '🇳🇷',
  'NP': '🇳🇵', 'NL': '🇳🇱', 'NC': '🇳🇨', 'NZ': '🇳🇿', 'NI': '🇳🇮', 'NE': '🇳🇪', 'NG': '🇳🇬', 'NU': '🇳🇺',
  'NF': '🇳🇫', 'MP': '🇲🇵', 'NO': '🇳🇴', 'OM': '🇴🇲', 'PK': '🇵🇰', 'PW': '🇵🇼', 'PS': '🇵🇸', 'PA': '🇵🇦',
  'PG': '🇵🇬', 'PY': '🇵🇾', 'PE': '🇵🇪', 'PH': '🇵🇭', 'PN': '🇵🇳', 'PL': '🇵🇱', 'PT': '🇵🇹', 'PR': '🇵🇷',
  'QA': '🇶🇦', 'RE': '🇷🇪', 'RO': '🇷🇴', 'RU': '🇷🇺', 'RW': '🇷🇼', 'BL': '🇧🇱', 'SH': '🇸🇭', 'KN': '🇰🇳',
  'LC': '🇱🇨', 'MF': '🇲🇫', 'PM': '🇵🇲', 'VC': '🇻🇨', 'WS': '🇼🇸', 'SM': '🇸🇲', 'ST': '🇸🇹', 'SA': '🇸🇦',
  'SN': '🇸🇳', 'RS': '🇷🇸', 'SC': '🇸🇨', 'SL': '🇸🇱', 'SG': '🇸🇬', 'SK': '🇸🇰', 'SI': '🇸🇮', 'SB': '🇸🇧',
  'SO': '🇸🇴', 'ZA': '🇿🇦', 'GS': '🇬🇸', 'SS': '🇸🇸', 'ES': '🇪🇸', 'LK': '🇱🇰', 'SD': '🇸🇩', 'SR': '🇸🇷',
  'SJ': '🇸🇯', 'SZ': '🇸🇿', 'SE': '🇸🇪', 'CH': '🇨🇭', 'SY': '🇸🇾', 'TW': '🇹🇼', 'TJ': '🇹🇯', 'TZ': '🇹🇿',
  'TH': '🇹🇭', 'TL': '🇹🇱', 'TG': '🇹🇬', 'TK': '🇹🇰', 'TO': '🇹🇴', 'TT': '🇹🇹', 'TN': '🇹🇳', 'TR': '🇹🇷',
  'TM': '🇹🇲', 'TC': '🇹🇨', 'TV': '🇹🇻', 'UG': '🇺🇬', 'UA': '🇺🇦', 'AE': '🇦🇪', 'GB': '🇬🇧', 'US': '🇺🇸',
  'UM': '🇺🇲', 'UY': '🇺🇾', 'UZ': '🇺🇿', 'VU': '🇻🇺', 'VE': '🇻🇪', 'VN': '🇻🇳', 'VG': '🇻🇬', 'VI': '🇻🇮',
  'WF': '🇼🇫', 'EH': '🇪🇭', 'YE': '🇾🇪', 'ZM': '🇿🇲', 'ZW': '🇿🇼',
  // 添加更多常见的国家代码变体
  'USA': '🇺🇸', 'CHN': '🇨🇳', 'HKG': '🇭🇰', 'GBR': '🇬🇧', 'JPN': '🇯🇵', 'DEU': '🇩🇪', 'FRA': '🇫🇷',
  'ITA': '🇮🇹', 'CAN': '🇨🇦', 'AUS': '🇦🇺', 'BRA': '🇧🇷', 'IND': '🇮🇳', 'RUS': '🇷🇺', 'KOR': '🇰🇷',
  'MEX': '🇲🇽', 'ESP': '🇪🇸', 'NLD': '🇳🇱', 'CHE': '🇨🇭', 'SWE': '🇸🇪', 'NOR': '🇳🇴', 'DNK': '🇩🇰',
  'FIN': '🇫🇮', 'POL': '🇵🇱', 'CZE': '🇨🇿', 'HUN': '🇭🇺', 'AUT': '🇦🇹', 'BEL': '🇧🇪', 'IRL': '🇮🇪',
  'PRT': '🇵🇹', 'GRC': '🇬🇷', 'TUR': '🇹🇷', 'ISR': '🇮🇱', 'SGP': '🇸🇬', 'THA': '🇹🇭', 'MYS': '🇲🇾',
  'IDN': '🇮🇩', 'PHL': '🇵🇭', 'VNM': '🇻🇳', 'PAK': '🇵🇰', 'BGD': '🇧🇩', 'LKA': '🇱🇰', 'NPL': '🇳🇵',
  'MMR': '🇲🇲', 'KHM': '🇰🇭', 'LAO': '🇱🇦', 'MNG': '🇲🇳', 'KAZ': '🇰🇿', 'UZB': '🇺🇿', 'TJK': '🇹🇯',
  'KGZ': '🇰🇬', 'TKM': '🇹🇲', 'AZE': '🇦🇿', 'GEO': '🇬🇪', 'ARM': '🇦🇲', 'MDA': '🇲🇩', 'UKR': '🇺🇦',
  'BLR': '🇧🇾', 'LTU': '🇱🇹', 'LVA': '🇱🇻', 'EST': '🇪🇪', 'SVN': '🇸🇮', 'HRV': '🇭🇷', 'BIH': '🇧🇦',
  'SRB': '🇷🇸', 'MNE': '🇲🇪', 'MKD': '🇲🇰', 'ALB': '🇦🇱', 'ROU': '🇷🇴', 'BGR': '🇧🇬', 'SVK': '🇸🇰'
};

// 国家代码到国家名称的映射
const COUNTRY_NAMES: { [key: string]: string } = {
  'AF': 'Afghanistan', 'AL': 'Albania', 'DZ': 'Algeria', 'AS': 'American Samoa', 'AD': 'Andorra',
  'AO': 'Angola', 'AI': 'Anguilla', 'AQ': 'Antarctica', 'AG': 'Antigua and Barbuda', 'AR': 'Argentina',
  'AM': 'Armenia', 'AW': 'Aruba', 'AU': 'Australia', 'AT': 'Austria', 'AZ': 'Azerbaijan',
  'BS': 'Bahamas', 'BH': 'Bahrain', 'BD': 'Bangladesh', 'BB': 'Barbados', 'BY': 'Belarus',
  'BE': 'Belgium', 'BZ': 'Belize', 'BJ': 'Benin', 'BM': 'Bermuda', 'BT': 'Bhutan',
  'BO': 'Bolivia', 'BA': 'Bosnia and Herzegovina', 'BW': 'Botswana', 'BV': 'Bouvet Island',
  'BR': 'Brazil', 'IO': 'British Indian Ocean Territory', 'BN': 'Brunei Darussalam', 'BG': 'Bulgaria',
  'BF': 'Burkina Faso', 'BI': 'Burundi', 'KH': 'Cambodia', 'CM': 'Cameroon', 'CA': 'Canada',
  'CV': 'Cape Verde', 'KY': 'Cayman Islands', 'CF': 'Central African Republic', 'TD': 'Chad',
  'CL': 'Chile', 'CN': 'China', 'CX': 'Christmas Island', 'CC': 'Cocos (Keeling) Islands',
  'CO': 'Colombia', 'KM': 'Comoros', 'CG': 'Congo', 'CD': 'Congo, Democratic Republic',
  'CK': 'Cook Islands', 'CR': 'Costa Rica', 'CI': 'Côte d\'Ivoire', 'HR': 'Croatia', 'CU': 'Cuba',
  'CY': 'Cyprus', 'CZ': 'Czech Republic', 'DK': 'Denmark', 'DJ': 'Djibouti', 'DM': 'Dominica',
  'DO': 'Dominican Republic', 'EC': 'Ecuador', 'EG': 'Egypt', 'SV': 'El Salvador',
  'GQ': 'Equatorial Guinea', 'ER': 'Eritrea', 'EE': 'Estonia', 'ET': 'Ethiopia',
  'FK': 'Falkland Islands', 'FO': 'Faroe Islands', 'FJ': 'Fiji', 'FI': 'Finland', 'FR': 'France',
  'GF': 'French Guiana', 'PF': 'French Polynesia', 'TF': 'French Southern Territories',
  'GA': 'Gabon', 'GM': 'Gambia', 'GE': 'Georgia', 'DE': 'Germany', 'GH': 'Ghana',
  'GI': 'Gibraltar', 'GR': 'Greece', 'GL': 'Greenland', 'GD': 'Grenada', 'GP': 'Guadeloupe',
  'GU': 'Guam', 'GT': 'Guatemala', 'GG': 'Guernsey', 'GN': 'Guinea', 'GW': 'Guinea-Bissau',
  'GY': 'Guyana', 'HT': 'Haiti', 'HM': 'Heard Island', 'VA': 'Holy See (Vatican City)',
  'HN': 'Honduras', 'HK': 'Hong Kong', 'HU': 'Hungary', 'IS': 'Iceland', 'IN': 'India',
  'ID': 'Indonesia', 'IR': 'Iran', 'IQ': 'Iraq', 'IE': 'Ireland', 'IM': 'Isle of Man',
  'IL': 'Israel', 'IT': 'Italy', 'JM': 'Jamaica', 'JP': 'Japan', 'JE': 'Jersey',
  'JO': 'Jordan', 'KZ': 'Kazakhstan', 'KE': 'Kenya', 'KI': 'Kiribati', 'KP': 'North Korea',
  'KR': 'South Korea', 'KW': 'Kuwait', 'KG': 'Kyrgyzstan', 'LA': 'Laos', 'LV': 'Latvia',
  'LB': 'Lebanon', 'LS': 'Lesotho', 'LR': 'Liberia', 'LY': 'Libya', 'LI': 'Liechtenstein',
  'LT': 'Lithuania', 'LU': 'Luxembourg', 'MO': 'Macao', 'MK': 'North Macedonia',
  'MG': 'Madagascar', 'MW': 'Malawi', 'MY': 'Malaysia', 'MV': 'Maldives', 'ML': 'Mali',
  'MT': 'Malta', 'MH': 'Marshall Islands', 'MQ': 'Martinique', 'MR': 'Mauritania',
  'MU': 'Mauritius', 'YT': 'Mayotte', 'MX': 'Mexico', 'FM': 'Micronesia', 'MD': 'Moldova',
  'MC': 'Monaco', 'MN': 'Mongolia', 'ME': 'Montenegro', 'MS': 'Montserrat', 'MA': 'Morocco',
  'MZ': 'Mozambique', 'MM': 'Myanmar', 'NA': 'Namibia', 'NR': 'Nauru', 'NP': 'Nepal',
  'NL': 'Netherlands', 'NC': 'New Caledonia', 'NZ': 'New Zealand', 'NI': 'Nicaragua',
  'NE': 'Niger', 'NG': 'Nigeria', 'NU': 'Niue', 'NF': 'Norfolk Island',
  'MP': 'Northern Mariana Islands', 'NO': 'Norway', 'OM': 'Oman', 'PK': 'Pakistan',
  'PW': 'Palau', 'PS': 'Palestine', 'PA': 'Panama', 'PG': 'Papua New Guinea',
  'PY': 'Paraguay', 'PE': 'Peru', 'PH': 'Philippines', 'PN': 'Pitcairn', 'PL': 'Poland',
  'PT': 'Portugal', 'PR': 'Puerto Rico', 'QA': 'Qatar', 'RE': 'Réunion', 'RO': 'Romania',
  'RU': 'Russia', 'RW': 'Rwanda', 'BL': 'Saint Barthélemy', 'SH': 'Saint Helena',
  'KN': 'Saint Kitts and Nevis', 'LC': 'Saint Lucia', 'MF': 'Saint Martin',
  'PM': 'Saint Pierre and Miquelon', 'VC': 'Saint Vincent and the Grenadines',
  'WS': 'Samoa', 'SM': 'San Marino', 'ST': 'Sao Tome and Principe', 'SA': 'Saudi Arabia',
  'SN': 'Senegal', 'RS': 'Serbia', 'SC': 'Seychelles', 'SL': 'Sierra Leone',
  'SG': 'Singapore', 'SK': 'Slovakia', 'SI': 'Slovenia', 'SB': 'Solomon Islands',
  'SO': 'Somalia', 'ZA': 'South Africa', 'GS': 'South Georgia', 'SS': 'South Sudan',
  'ES': 'Spain', 'LK': 'Sri Lanka', 'SD': 'Sudan', 'SR': 'Suriname',
  'SJ': 'Svalbard and Jan Mayen', 'SZ': 'Eswatini', 'SE': 'Sweden', 'CH': 'Switzerland',
  'SY': 'Syria', 'TW': 'Taiwan', 'TJ': 'Tajikistan', 'TZ': 'Tanzania', 'TH': 'Thailand',
  'TL': 'Timor-Leste', 'TG': 'Togo', 'TK': 'Tokelau', 'TO': 'Tonga',
  'TT': 'Trinidad and Tobago', 'TN': 'Tunisia', 'TR': 'Turkey', 'TM': 'Turkmenistan',
  'TC': 'Turks and Caicos Islands', 'TV': 'Tuvalu', 'UG': 'Uganda', 'UA': 'Ukraine',
  'AE': 'United Arab Emirates', 'GB': 'United Kingdom', 'US': 'United States',
  'UM': 'United States Minor Outlying Islands', 'UY': 'Uruguay', 'UZ': 'Uzbekistan',
  'VU': 'Vanuatu', 'VE': 'Venezuela', 'VN': 'Vietnam', 'VG': 'British Virgin Islands',
  'VI': 'U.S. Virgin Islands', 'WF': 'Wallis and Futuna', 'EH': 'Western Sahara',
  'YE': 'Yemen', 'ZM': 'Zambia', 'ZW': 'Zimbabwe',
  // 添加更多常见的国家代码变体
  'USA': 'United States', 'CHN': 'China', 'HKG': 'Hong Kong', 'GBR': 'United Kingdom', 
  'JPN': 'Japan', 'DEU': 'Germany', 'FRA': 'France', 'ITA': 'Italy', 'CAN': 'Canada', 
  'AUS': 'Australia', 'BRA': 'Brazil', 'IND': 'India', 'RUS': 'Russia', 'KOR': 'South Korea',
  'MEX': 'Mexico', 'ESP': 'Spain', 'NLD': 'Netherlands', 'CHE': 'Switzerland', 'SWE': 'Sweden', 
  'NOR': 'Norway', 'DNK': 'Denmark', 'FIN': 'Finland', 'POL': 'Poland', 'CZE': 'Czech Republic', 
  'HUN': 'Hungary', 'AUT': 'Austria', 'BEL': 'Belgium', 'IRL': 'Ireland', 'PRT': 'Portugal', 
  'GRC': 'Greece', 'TUR': 'Turkey', 'ISR': 'Israel', 'SGP': 'Singapore', 'THA': 'Thailand', 
  'MYS': 'Malaysia', 'IDN': 'Indonesia', 'PHL': 'Philippines', 'VNM': 'Vietnam', 'PAK': 'Pakistan', 
  'BGD': 'Bangladesh', 'LKA': 'Sri Lanka', 'NPL': 'Nepal', 'MMR': 'Myanmar', 'KHM': 'Cambodia', 
  'LAO': 'Laos', 'MNG': 'Mongolia', 'KAZ': 'Kazakhstan', 'UZB': 'Uzbekistan', 'TJK': 'Tajikistan',
  'KGZ': 'Kyrgyzstan', 'TKM': 'Turkmenistan', 'AZE': 'Azerbaijan', 'GEO': 'Georgia', 'ARM': 'Armenia', 
  'MDA': 'Moldova', 'UKR': 'Ukraine', 'BLR': 'Belarus', 'LTU': 'Lithuania', 'LVA': 'Latvia', 
  'EST': 'Estonia', 'SVN': 'Slovenia', 'HRV': 'Croatia', 'BIH': 'Bosnia and Herzegovina', 
  'SRB': 'Serbia', 'MNE': 'Montenegro', 'MKD': 'North Macedonia', 'ALB': 'Albania', 'ROU': 'Romania', 
  'BGR': 'Bulgaria', 'SVK': 'Slovakia'
};

const CountryFlag: React.FC<CountryFlagProps> = ({ countryCode, showName = false }) => {
  if (!countryCode) {
    return <span>-</span>;
  }

  const upperCode = countryCode.toUpperCase();
  const flag = COUNTRY_FLAGS[upperCode] || '🏳️';
  const countryName = COUNTRY_NAMES[upperCode] || countryCode;

  return (
    <Space>
      <span style={{ fontSize: '16px' }}>{flag}</span>
      <span>{upperCode}</span>
      {showName && (
        <span style={{ color: '#666', fontSize: '12px' }}>({countryName})</span>
      )}
    </Space>
  );
};

export default CountryFlag;
