# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['main.py'],  # 将 'main.py' 替换为您的主脚本文件名
             pathex=[],
             binaries=[],
             datas=[
                 ('MapleMonoNL-Regular.ttf', '.'),
                 ('Cinese.ttf', '.'),
                 ('icon.ico', '.')
             ],
             hiddenimports=['pkg_resources.py2_warn', 'PIL.ImageTk', 'PyQt5.sip'],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=["torch"],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='QSL_Manager',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=False,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None,
          icon='icon.ico')
