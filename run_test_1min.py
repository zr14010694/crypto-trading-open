#!/usr/bin/env python3
"""1分钟实盘测试运行脚本"""
import asyncio
import sys
import os
from pathlib import Path

# SSL 证书修复
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / '.env')

from core.services.arbitrage_monitor_v2.core.unified_orchestrator import UnifiedOrchestrator
from core.services.arbitrage_monitor_v2.config.debug_config import DebugConfig

RUN_SECONDS = 180


async def main():
    debug_config = DebugConfig.create_production()
    orchestrator = None
    try:
        orchestrator = UnifiedOrchestrator(
            segmented_config_path=Path("config/arbitrage/arbitrage_segmented.yaml"),
            monitor_config_path=Path("config/arbitrage/monitor_v2.yaml"),
            debug_config=debug_config,
        )

        # start() 包含永久循环，用 task 包装并限时
        start_task = asyncio.create_task(orchestrator.start())

        print(f"\n{'='*60}")
        print(f"⏱️  系统运行中... 将在 {RUN_SECONDS} 秒后自动停止")
        print(f"{'='*60}\n")

        await asyncio.sleep(RUN_SECONDS)
        start_task.cancel()

    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if orchestrator and hasattr(orchestrator, 'stop'):
            print("\n🛑 停止中...")
            await orchestrator.stop()
        print("✅ 测试运行完成")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ 已退出")
