# fdrawcmd.sys


## Introduction

[fdrawcmd.sys](http://simonowen.com/fdrawcmd/) is a Windows filter driver that exposes command-level access to the on-board floppy disk controller, making it possible to read/write many non-standard and copy-protected disk formats. The driver runs under Windows 2000 or later.


## Development

There has been no new development in the last 5 years, and the driver is considered mostly complete for its original task. The majority of the code is over 10 years old now, and showing its age in places. Rather than risk refactoring it I've decided to release it as-is.

The bulk of the driver code validates and queues requests, which are ultimately passed down the driver stack to Microsoft's FDC.SYS driver, which manages the FDC chip. It uses the same private interface as the system FLPYDISK.SYS driver, which supports a range of DOS-compatible disk formats.

Replicating some copy-protection formats requires lower-level access than the FDC.SYS interface provides. For example, interrupting disk writes requires tracking the write position on the disk, so it can be stopped at the correct point. FDC.SYS doesn't expose its DMA ADAPTER_OBJECT, so we must use some slightly underhand tricks to obtain it. Similarly, the FDC.SYS command table has disabled read/write commands that use deleted data address marks, so we must locate and patch them to restore the missing functionality. There may be other naughty stuff I've since forgotten.


## Building

If you need the driver to run on Windows 7 or later only, I recommend building using Visual studio 2015 (the free [Community edition](https://www.visualstudio.com/products/visual-studio-community-vs) works fine). You'll also need a recent WDK installed, such as the [WDK 10](https://msdn.microsoft.com/en-us/library/windows/hardware/ff557573(v=vs.85).aspx). The build automatically signs the driver with a test code-signing certificate.

For complete compatibility with Windows 2000 (or later) you'll need to use the Windows Server 2003 DDK (6001.18002), and build from the command-line. You'll also need to manually test sign the driver.


## Installing

To install the driver requires registering it with the Service Control Manager, installing it as a lower filter under floppy disk class devices in the registry, then restarting the floppy device stack. That can all be performed manually, but I don't have detailed instructions to hand.

An easier option is to perform a one-time install of the [official driver](http://simonowen.com/fdrawcmd) from my website using [FdInstall.exe](http://simonowen.com/fdrawcmd/FdInstall.exe), then replace the driver binary in `Windows\System32\Drivers\`.

Starting with Windows Vista x64, kernel-mode drivers must be properly code-signed or Windows will refuse to load them. Despite the built driver containing a test signature, you'll still need to [disable driver signature enforcement](http://www.howtogeek.com/167723/how-to-disable-driver-signature-verification-on-64-bit-windows-8.1-so-that-you-can-install-unsigned-drivers/) so Windows will accept it. Note that this only works for one boot, so it's not a permanent solution.

Each time you update the driver you'll need to restart the floppy device stack to reload it.  Open *Device Manager* and expand *Floppy drive controllers* to find *Standard floppy disk controller*. Right-click on it and Disable it, then Enable it. If the device won't start you probably forgot to disable driver signature enforcement!


## License

The fdrawcmd.sys source code is released under the [MIT license](https://tldrlegal.com/license/mit-license).


## Contact

Simon Owen
[http://simonowen.com](http://simonowen.com)
