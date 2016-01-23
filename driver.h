#ifndef DRIVER_H
#define DRIVER_H

#define DRIVERNAME "FDRAWCMD"			// for use in messages

///////////////////////////////////////////////////////////////////////////////
// Device extension structure

enum { FIDO_EXTENSION, EDO_EXTENSION };

typedef struct _COMMON_DEVICE_EXTENSION
{
	BOOLEAN IsEdo;
} COMMON_DEVICE_EXTENSION, *PCOMMON_DEVICE_EXTENSION;

typedef struct _DEVICE_EXTENSION : public _COMMON_DEVICE_EXTENSION
{
	struct _EXTRA_DEVICE_EXTENSION* edx;

	PDEVICE_OBJECT DeviceObject;			// device object this extension belongs to
	PDEVICE_OBJECT LowerDeviceObject;		// next lower driver in same stack
	PDEVICE_OBJECT Pdo;						// the PDO
	IO_REMOVE_LOCK RemoveLock;

	UCHAR DeviceUnit;

} DEVICE_EXTENSION, *PDEVICE_EXTENSION;

typedef struct _EXTRA_DEVICE_EXTENSION: public _COMMON_DEVICE_EXTENSION
{
	PDEVICE_EXTENSION pdx;
	PDEVICE_OBJECT DeviceObject;
	UNICODE_STRING SymbolicName;

	BOOLEAN killthread;
	KSEMAPHORE semaphore;
	PKTHREAD thread;

	KEVENT dmasync;
	PKTHREAD dmathread;
	BOOLEAN StopDmaThread;
	PDMA_ADAPTER* ppAdapterObject;
	ULONG MaxTransferSize;

	NTSTATUS Status;
	IO_CSQ IrpQueue;
	LIST_ENTRY IrpQueueAnchor;
	KSPIN_LOCK IrpQueueLock;

	PUCHAR IoBuffer;
	PMDL IoBufferMdl;
	ULONG IoBufferSize;

	UCHAR FifoIn[16];
	UCHAR FifoOut[16];

	BOOLEAN Acquired;
	BOOLEAN Enabled;
	BOOLEAN NeedReset;
	BOOLEAN NeedInit;
	BOOLEAN NeedRecalibrate;
	BOOLEAN NeedDisk;

	ULONG SpinTime;
	LONG AcquireCount;

	ULONG PhysCyl;
	UCHAR DeviceUnit;
	UCHAR DriveOnValue;

// Drive-specific configuration below

	BOOLEAN WaitSector;
	UCHAR WaitSectorCount;

	BOOLEAN ShortWrite;
	ULONG DmaThreshold;
	ULONG DmaDelay;

	UCHAR MotorTimeout;
//	USHORT MotorSettleTimeRead;
	USHORT MotorSettleTimeWrite;
	UCHAR HeadSettleTime;

	UCHAR EisEfifoPollFifothr;			// Configure
	UCHAR PreTrk;						// Configure

	UCHAR StepRateHeadUnloadTime;		// Specify
	UCHAR HeadLoadTimeNoDMA;			// Specify

	UCHAR PerpendicularMode;
	UCHAR DataRate;

} EXTRA_DEVICE_EXTENSION, *PEXTRA_DEVICE_EXTENSION;

///////////////////////////////////////////////////////////////////////////////
// Global functions

VOID RemoveDevice(IN PDEVICE_OBJECT fdo);
NTSTATUS CompleteRequest(IN PIRP Irp, IN NTSTATUS status, IN ULONG_PTR info=0, IN CCHAR boost=IO_NO_INCREMENT);

#endif // DRIVER_H
