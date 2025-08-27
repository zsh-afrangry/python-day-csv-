#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通达信.day文件解析器 - 修正完整版
将通达信的日线数据文件（.day）转换为CSV文件。

支持Python 3.9+
需要安装pandas库: pip install pandas
"""

import struct
import pandas as pd
from datetime import datetime
import os


def parse_tdx_day_file(file_path):
    """
    解析单个通达信.day文件。

    Args:
        file_path (str): .day文件路径。

    Returns:
        pandas.DataFrame or None: 包含股票历史数据的DataFrame，如果失败则返回None。
    """

    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误: 文件不存在 -> {file_path}")
        return None

    # 从文件名提取股票代码
    stock_code = os.path.basename(file_path).replace('.day', '')
    print(f"--- 开始解析股票: {stock_code} ---")

    data_list = []

    try:
        with open(file_path, 'rb') as f:
            # 每条记录固定为32字节
            record_size = 32

            while True:
                buffer = f.read(record_size)
                # 如果读取的字节数不足32，说明到达文件末尾
                if len(buffer) < record_size:
                    break

                try:
                    # 【修正】使用正确的格式一次性解包
                    # 格式: < (小端字节序)
                    #       I (日期, 4字节无符号整数)
                    #       I (开盘价*100, 4字节无符号整数)
                    #       I (最高价*100, 4字节无符号整数)
                    #       I (最低价*100, 4字节无符号整数)
                    #       I (收盘价*100, 4字节无符号整数)
                    #       f (成交额, 4字节浮点数)
                    #       I (成交量, 4字节无符号整数)
                    #       I (保留字段, 4字节无符号整数)
                    unpacked_data = struct.unpack('<IIIIIfII', buffer)

                    date_int = unpacked_data[0]

                    # 【修正】使用简单、正确的方式解析日期
                    # 通达信日期格式为YYYYMMDD的整数
                    try:
                        date_str = datetime.strptime(str(date_int), '%Y%m%d').strftime('%Y-%m-%d')
                    except ValueError:
                        print(f"警告: 发现无效日期格式 {date_int}，跳过该条记录。")
                        continue

                    # 价格需要除以100还原
                    open_price = unpacked_data[1] / 100.0
                    high_price = unpacked_data[2] / 100.0
                    low_price = unpacked_data[3] / 100.0
                    close_price = unpacked_data[4] / 100.0
                    amount = unpacked_data[5]
                    volume = unpacked_data[6]

                    # 将解析出的数据添加到列表
                    data_list.append({
                        'date': date_str,
                        'stock_code': stock_code,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume,
                        'amount': amount
                    })

                except struct.error:
                    print("警告: 数据记录解析错误，可能文件已损坏或到达末尾。")
                    continue

    except Exception as e:
        print(f"读取文件时发生严重错误: {e}")
        return None

    # 如果列表为空，说明没有解析到任何有效数据
    if not data_list:
        print("未解析到任何有效数据。")
        return None

    # 将列表转换为Pandas DataFrame
    df = pd.DataFrame(data_list)
    # 将date列转换为标准的日期时间格式
    df['date'] = pd.to_datetime(df['date'])
    # 确保数据按日期升序排列
    df = df.sort_values(by='date', ascending=True)
    # 重置索引
    df = df.reset_index(drop=True)

    print(f"成功解析 {len(df)} 条记录")
    if not df.empty:
        print(f"数据时间范围: {df['date'].min().strftime('%Y-%m-%d')} 到 {df['date'].max().strftime('%Y-%m-%d')}")

    return df


def save_to_csv(df, output_path):
    """
    将DataFrame保存为CSV文件。

    Args:
        df (pandas.DataFrame): 待保存的数据。
        output_path (str): 输出CSV文件的路径。
    """
    if df is not None and not df.empty:
        try:
            # 使用 utf-8-sig 编码确保在Excel中打开不会乱码
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"数据已成功保存到: {output_path}")
        except Exception as e:
            print(f"保存CSV文件时出错: {e}")
    else:
        print("没有数据可以保存。")


# --- 主函数入口 ---
if __name__ == "__main__":

    # --- 配置区域 ---
    # 请将这里的路径修改为您自己的.day文件路径
    # 示例:
    # Windows: "C:\\new_tdxqh\\vipdoc\\sh\\lday\\sh000001.day"
    # Linux/Mac: "/data/tdx/sh/lday/sh000001.day"
    input_file_path = "sh000001.day"  # 【请修改】输入.day文件路径

    # 设置输出的CSV文件名
    output_file_path = "sh000001.csv"  # 【可修改】输出CSV文件路径
    # --- 配置结束 ---

    # 1. 解析文件
    stock_df = parse_tdx_day_file(input_file_path)

    # 2. 如果解析成功，则保存为CSV
    if stock_df is not None:
        save_to_csv(stock_df, output_file_path)

        # 3. 显示前5行数据作为预览
        print("\n--- 数据预览 (前5行) ---")
        print(stock_df.head())

        # 4. 显示末5行数据作为预览
        print("\n--- 数据预览 (后5行) ---")
        print(stock_df.tail())