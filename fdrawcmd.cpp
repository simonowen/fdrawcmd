// FDRAWCMD.SYS, written by Simon Owen

#include "stddcls.h"
#include "driver.h"
#include "fdrawcmd.h"
#include "version.h"

#pragma LOCKEDCODE
#pragma LOCKEDDATA

#define POOL_TAG		' rdF'

const ULONG MAX_CYLS = 86;

#define SECONDS(x)		(-(LONGLONG(x) * 1000 * 10000))
#define MILLISECONDS(x) (-(LONGLONG(x) * 10000))


#define CONTROL_DEVICE			L"\\Device\\fdrawcmd"
#define CONTROL_LINK			L"\\DosDevices\\fdrawcmd"
#define DEVICE_TEMPLATE 		L"\\Device\\fdraw%u"
#define LINK_TEMPLATE			L"\\DosDevices\\fdraw%u"

#define IOCTL_DISK_BASE 					FILE_DEVICE_DISK
#define DRVCTL_ENABLE_CONTROLLER			0x04
#define DRVCTL_ENABLE_DMA_AND_INTERRUPTS	0x08
#define FDC_TIMEOUT 						4


#define COMMAND_MASK		0x1f
#define FDC_NO_DATA 		0x00
#define FDC_READ_DATA		0x01
#define FDC_WRITE_DATA		0x02


// Legacy FDC addresses used for direct port access
PUCHAR FDC_DOR_PORT  = (PUCHAR)0x03f2;	// Drive Output Register
PUCHAR FDC_MSR_PORT  = (PUCHAR)0x03f4;	// Main Status Register
PUCHAR FDC_FIFO_PORT = (PUCHAR)0x03f5;	// Data Register (FIFO)
PUCHAR FDC_DSR_PORT  = (PUCHAR)0x03f7;	// Data Rate Select Register


// Bits in the DISK_CHANGE register
#define DSKCHG_RESERVED 				   0x7f
#define DSKCHG_DISKETTE_REMOVED 		   0x80

// Bits in status register 0
#define STREG0_DRIVE_0					   0x00
#define STREG0_DRIVE_1					   0x01
#define STREG0_DRIVE_2					   0x02
#define STREG0_DRIVE_3					   0x03
#define STREG0_HEAD 					   0x04
#define STREG0_DRIVE_NOT_READY			   0x08
#define STREG0_DRIVE_FAULT				   0x10
#define STREG0_SEEK_COMPLETE			   0x20
#define STREG0_END_NORMAL				   0x00
#define STREG0_END_ERROR				   0x40
#define STREG0_END_INVALID_COMMAND		   0x80
#define STREG0_END_DRIVE_NOT_READY		   0xC0
#define STREG0_END_MASK 				   0xC0

// Bits in status register 1
#define STREG1_ID_NOT_FOUND 			   0x01
#define STREG1_WRITE_PROTECTED			   0x02
#define STREG1_SECTOR_NOT_FOUND 		   0x04
#define STREG1_RESERVED1				   0x08
#define STREG1_DATA_OVERRUN 			   0x10
#define STREG1_CRC_ERROR				   0x20
#define STREG1_RESERVED2				   0x40
#define STREG1_END_OF_DISKETTE			   0x80

// Bits in status register 2
#define STREG2_SUCCESS					   0x00
#define STREG2_DATA_NOT_FOUND			   0x01
#define STREG2_BAD_CYLINDER 			   0x02
#define STREG2_SCAN_FAIL				   0x04
#define STREG2_SCAN_EQUAL				   0x08
#define STREG2_WRONG_CYLINDER			   0x10
#define STREG2_CRC_ERROR				   0x20
#define STREG2_DELETED_DATA 			   0x40
#define STREG2_RESERVED 				   0x80

// Bits in status register 3
#define STREG3_DRIVE_0					   0x00
#define STREG3_DRIVE_1					   0x01
#define STREG3_DRIVE_2					   0x02
#define STREG3_DRIVE_3					   0x03
#define STREG3_HEAD 					   0x04
#define STREG3_TWO_SIDED				   0x08
#define STREG3_TRACK_0					   0x10
#define STREG3_DRIVE_READY				   0x20
#define STREG3_WRITE_PROTECTED			   0x40
#define STREG3_DRIVE_FAULT				   0x80

extern "C"
{
// This header is part of an old Windows DDK and contains the structures and
// definitions needed to use FDC.SYS. Copy it to the project directory.
// I can't include it with the source, but Google may be able you with that.
#include "ntddfdc.h"
}


typedef struct _COMMAND_TABLE
{
	UCHAR	OpCode;
	UCHAR	NumberOfParameters;
	UCHAR	FirstResultByte;
	UCHAR	NumberOfResultBytes;
	BOOLEAN InterruptExpected;
	BOOLEAN AlwaysImplemented;
	UCHAR	DataTransfer;
} COMMAND_TABLE, *PCOMMAND_TABLE;

#define COMMAND_TABLE_ENTRIES	29


#ifdef _PREFAST_
DRIVER_ADD_DEVICE AddDevice;
DRIVER_UNLOAD DriverUnload;
DRIVER_DISPATCH DispatchAny;
DRIVER_DISPATCH DispatchPower;
DRIVER_DISPATCH DispatchPnp;
IO_COMPLETION_ROUTINE StartDeviceCompletionRoutine;
IO_COMPLETION_ROUTINE UsageNotificationCompletionRoutine;
#endif

NTSTATUS AddDevice (IN PDRIVER_OBJECT DriverObject, IN PDEVICE_OBJECT pdo);
VOID DriverUnload (IN PDRIVER_OBJECT fido);
NTSTATUS DispatchAny (IN PDEVICE_OBJECT fido, IN PIRP Irp);
NTSTATUS DispatchPower (IN PDEVICE_OBJECT fido, IN PIRP Irp);
NTSTATUS DispatchPnp (IN PDEVICE_OBJECT fido, IN PIRP Irp);
NTSTATUS StartDeviceCompletionRoutine (PDEVICE_OBJECT fido, PIRP Irp, PVOID context);
NTSTATUS UsageNotificationCompletionRoutine (PDEVICE_OBJECT fido, PIRP Irp, PVOID context);

NTSTATUS StartThread (PEXTRA_DEVICE_EXTENSION edx);
VOID StopThread (PEXTRA_DEVICE_EXTENSION edx);
VOID ThreadProc (PVOID StartContext);
VOID ShortCommandThreadProc (PVOID StartContext);

NTSTATUS FlFdcDeviceIo (IN PDEVICE_OBJECT DeviceObject, IN ULONG Ioctl, IN OUT PVOID Data);
NTSTATUS GetFdcInfo (PEXTRA_DEVICE_EXTENSION edx, PFDC_INFO pfdci);
NTSTATUS AcquireFdc (PEXTRA_DEVICE_EXTENSION edx, ULONG Seconds);
NTSTATUS ReleaseFdc (PEXTRA_DEVICE_EXTENSION edx);

void EnableUnit (PEXTRA_DEVICE_EXTENSION edx, BOOLEAN fInterrupts_);
void DisableUnit (PEXTRA_DEVICE_EXTENSION edx, BOOLEAN fInterrupts_);

///////////////////////////////////////////////////////////////////////////////

#define GET_DEVICE_EXTENSION(csq)	CONTAINING_RECORD(csq, EXTRA_DEVICE_EXTENSION, IrpQueue)

#pragma prefast(suppress:28167, "The IRQL is restored in ReleaseLock()")
VOID AcquireLock (PIO_CSQ csq, PKIRQL Irql)
{
	PEXTRA_DEVICE_EXTENSION edx = GET_DEVICE_EXTENSION(csq);
#pragma prefast(suppress:8103, "The CSQ spinlock is released in ReleaseLock()")
	KeAcquireSpinLock(&edx->IrpQueueLock, Irql);
}

#pragma prefast(suppress:28167, "The IRQL was set in AcquireLock()")
VOID ReleaseLock (PIO_CSQ csq, KIRQL Irql)
{
	PEXTRA_DEVICE_EXTENSION edx = GET_DEVICE_EXTENSION(csq);
#pragma prefast(suppress:8107 28107, "The CSQ spinlock is allocated and acquired in AcquireLock()")
	KeReleaseSpinLock(&edx->IrpQueueLock, Irql);
}

VOID InsertIrp (PIO_CSQ csq, PIRP Irp)
{
	PEXTRA_DEVICE_EXTENSION edx = GET_DEVICE_EXTENSION(csq);

	InsertTailList(&edx->IrpQueueAnchor, &Irp->Tail.Overlay.ListEntry);
}

PIRP PeekNextIrp (PIO_CSQ csq, PIRP Irp, PVOID /*PeekContext*/)
{
	PEXTRA_DEVICE_EXTENSION edx = GET_DEVICE_EXTENSION(csq);
	PLIST_ENTRY next = Irp ? Irp->Tail.Overlay.ListEntry.Flink : edx->IrpQueueAnchor.Flink;

	if (next == &edx->IrpQueueAnchor)
		return NULL;

	return CONTAINING_RECORD(next, IRP, Tail.Overlay.ListEntry);
}

VOID RemoveIrp (PIO_CSQ /*csq*/, PIRP Irp)
{
	RemoveEntryList(&Irp->Tail.Overlay.ListEntry);
}

VOID CompleteCanceledIrp (PIO_CSQ /*csq*/, PIRP Irp)
{
	CompleteRequest(Irp, STATUS_CANCELLED);
}

///////////////////////////////////////////////////////////////////////////////

UCHAR* memmem (void* s, size_t ns, UCHAR* f, size_t nf)
{
	UCHAR *p = (UCHAR*)s, *e = p + ns - nf, b = *f;

	while (p < e)
	{
		if ((*p++ == b) && !memcmp(p-1, f, nf))
			return p-1;
	}

	return NULL;
}

///////////////////////////////////////////////////////////////////////////////

FAST_MUTEX ControlMutex;
LONG InstanceCount;
PDEVICE_OBJECT ControlDeviceObject;

PCOMMAND_TABLE pCommandTable, pOrigCommandTable;
LONG PatchCount;


#pragma prefast(suppress:28101, "We know this is a driver")
extern "C" NTSTATUS DriverEntry (IN PDRIVER_OBJECT DriverObject, IN PUNICODE_STRING /*RegistryPath*/)
{
	NTSTATUS status = STATUS_SUCCESS;
	KdPrint((DRIVERNAME " - Entering DriverEntry: DriverObject %p\n", DriverObject));

	// Initialize function pointers

	DriverObject->DriverUnload = DriverUnload;
	DriverObject->DriverExtension->AddDevice = AddDevice;

	for (int i = 0; i < arraysize(DriverObject->MajorFunction); ++i)
		DriverObject->MajorFunction[i] = DispatchAny;

	DriverObject->MajorFunction[IRP_MJ_POWER] = DispatchPower;
	DriverObject->MajorFunction[IRP_MJ_PNP] = DispatchPnp;

	ExInitializeFastMutex (&ControlMutex);

	return status;
}

///////////////////////////////////////////////////////////////////////////////

VOID DriverUnload (IN PDRIVER_OBJECT DriverObject)
{
	DriverObject;
	KdPrint((DRIVERNAME " - Entering DriverUnload: DriverObject %p\n", DriverObject));
}

///////////////////////////////////////////////////////////////////////////////

PDMA_ADAPTER* FindAdapterObject (PDEVICE_OBJECT pdo)
{
	// Walk the object stack to find the controller
#pragma prefast(suppress:28175, "Finding the controller object requires accessing NextDevice here")
	for (pdo = pdo->DriverObject->DeviceObject ; pdo && pdo->DeviceType != FILE_DEVICE_CONTROLLER ; pdo = pdo->NextDevice) ;

	if (!pdo)
	{
		KdPrint(("!!! FindAdapterObject: failed to find controller device\n"));
		return NULL;
	}

	PUINT_PTR pp = (PUINT_PTR)pdo->DeviceExtension;
	KdPrint(("FDC device object is %p, with extension at %p\n", pdo, pp));

	// Look through the first 128 pointer-sized slots in the extension
	for (int i = 2 ; i < 128 ; i++)
	{
		// Check for the driver object in the current slot
#pragma prefast(suppress:6385, "Accessing pointer as array, prefast shouldn't attempt size validation!")
		if (pp[i] == (UINT_PTR)pdo->DriverObject)
		{
			KdPrint(("Possible DriverObject (%p) found at %p\n", pdo->DriverObject, &pp[i]));

			for (int j = i+1 ; j < 128 ; j++)
			{
				// Skip NULL entries, as found on W2K for structure alignment padding
				if (!pp[j])
				{
					KdPrint((" skipping structure alignment padding at %p\n", &pp[j]));
					continue;
				}

				// If we don't find consecutive addresses in the CONTROLLER structure, we're in the wrong place
				if ((pp[j]+1) != pp[j+1])
				{
					KdPrint(("Controller address mismatch: %lX %lX ...\n", pp[j], pp[j+1]));
					break;
				}

				KdPrint(("### AdapterObject (%lX) found at %p\n", pp[i-2], &pp[i-2]));
				return (PDMA_ADAPTER*)&pp[i-2];
			}
		}
	}

	KdPrint(("!!! FindAdapterObject: failed to find adapter object in extension!\n"));
	return NULL;
}

NTSTATUS PatchCommandTable (PDEVICE_OBJECT pdo)
{
	if (InterlockedIncrement(&PatchCount) != 1)
		return STATUS_SUCCESS;

	static COMMAND_TABLE orig_ct = { 0x06, 8, 1, 7, TRUE, TRUE, FDC_READ_DATA };
#pragma prefast(suppress:28175, "I have no choice but to access DriverStart here")
	pCommandTable = (PCOMMAND_TABLE)memmem(pdo->DriverObject->DriverStart, pdo->DriverObject->DriverSize, (UCHAR*)&orig_ct, sizeof(orig_ct));
	if (!pCommandTable)
		return STATUS_RESOURCE_DATA_NOT_FOUND;

	KdPrint(("FDC Command Table found at %p\n", pCommandTable));
	pOrigCommandTable = (PCOMMAND_TABLE)ExAllocatePoolWithTag(NonPagedPool, sizeof(COMMAND_TABLE)*COMMAND_TABLE_ENTRIES, POOL_TAG);
	if (!pOrigCommandTable)
		return STATUS_INSUFFICIENT_RESOURCES;

	RtlCopyMemory(pOrigCommandTable, pCommandTable, sizeof(COMMAND_TABLE)*COMMAND_TABLE_ENTRIES);

	for (int i = 0 ; i < COMMAND_TABLE_ENTRIES ; i++)
	{
		switch (pCommandTable[i].OpCode)
		{
			case 0x0c:	// read deleted data
			{
				COMMAND_TABLE ct = { 0x0c, 8, 1, 7,  TRUE, TRUE, FDC_READ_DATA };
				RtlCopyMemory(&pCommandTable[i], &ct, sizeof(ct));
				break;
			}

			case 0x09:	// write deleted data
			{
				COMMAND_TABLE ct = { 0x09, 8, 1, 7,  TRUE, TRUE, FDC_WRITE_DATA };
				RtlCopyMemory(&pCommandTable[i], &ct, sizeof(ct));
				break;
			}

			case 0xad:	// format and write
			{
				COMMAND_TABLE ct = { 0xef, 5, 1, 7,  TRUE, TRUE, FDC_WRITE_DATA };
				RtlCopyMemory(&pCommandTable[i], &ct, sizeof(ct));
				break;
			}
		}
	}

	return STATUS_SUCCESS;
}

NTSTATUS UnpatchCommandTable ()
{
	if (!InterlockedDecrement(&PatchCount) && pCommandTable)
	{
		if (pOrigCommandTable)
		{
			RtlCopyMemory(pCommandTable, pOrigCommandTable, sizeof(COMMAND_TABLE)*COMMAND_TABLE_ENTRIES);
			ExFreePool(pOrigCommandTable);
			pOrigCommandTable = NULL;
		}

		pCommandTable = NULL;
	}

	return STATUS_SUCCESS;
}

BYTE SizeCode (BYTE size_)
{
	return (size_ > 8) ? 8 : size_;
}

ULONG SectorSize (BYTE size_)
{
	return 128U << SizeCode(size_);
}

void SetDefaults (PEXTRA_DEVICE_EXTENSION edx)
{
	KdPrint(("### SetDefaults for drive\n"));

	edx->NeedReset = TRUE;
	edx->NeedDisk = TRUE;

	edx->WaitSector = FALSE;
	edx->WaitSectorCount = 0;

	edx->ShortWrite = FALSE;
	edx->DmaDelay = 0;
	edx->DmaThreshold = 0;

	edx->MotorTimeout = 2;
	edx->SpinTime = 0;

//	edx->MotorSettleTimeRead = 500; 		// settle time in ms after turning motor on before a read
	edx->MotorSettleTimeWrite = 1000;		// settle time in ms after turning motor on before a write
	edx->HeadSettleTime = 0x0f; 			// head settle time after seek (0x0f for 3.5", 0x1f for 3")

	edx->EisEfifoPollFifothr = 0x1f;
	edx->PreTrk = 0x00;
	edx->StepRateHeadUnloadTime = 0xdf;
	edx->HeadLoadTimeNoDMA = 0x02;
	edx->PerpendicularMode = 0x00;
	edx->DataRate = 0x02;

	edx->DeviceUnit = edx->pdx->DeviceUnit;
	edx->DriveOnValue = (FDC_MOTOR_A << edx->DeviceUnit) | (FDC_SELECT_A + edx->DeviceUnit);
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS CreateControlObject (IN PDEVICE_OBJECT DeviceObject)
{
	KdPrint(("Entering CreateControlObject\n"));

	UNICODE_STRING ntDeviceName, symbolicLinkName;
	NTSTATUS status = STATUS_SUCCESS;

	ExAcquireFastMutexUnsafe(&ControlMutex);
	KdPrint(("ControlDeviceObject = %p\n", ControlDeviceObject));

	// If this is a first instance of the device, then create a controlobject
	if (++InstanceCount == 1)
	{
		// Initialize the unicode strings
		RtlInitUnicodeString(&ntDeviceName, CONTROL_DEVICE);
		RtlInitUnicodeString(&symbolicLinkName, CONTROL_LINK);

		// Create a named deviceobject so that applications or drivers
		// can directly talk to us without going through the entire stack.
		// This call could fail if there are not enough resources or
		// another deviceobject of same name exists (name collision).
		status = IoCreateDevice(DeviceObject->DriverObject, 0, &ntDeviceName, FILE_DEVICE_UNKNOWN, FILE_DEVICE_SECURE_OPEN, FALSE, &ControlDeviceObject);
		KdPrint(("ControlDeviceObject created: %p\n", ControlDeviceObject));

		if (NT_SUCCESS(status))
		{
			ControlDeviceObject->Flags |= DO_BUFFERED_IO;

			status = IoCreateSymbolicLink(&symbolicLinkName, &ntDeviceName);

			if (NT_SUCCESS(status))
				ControlDeviceObject->Flags &= ~DO_DEVICE_INITIALIZING;
			else
			{
				IoDeleteDevice(ControlDeviceObject);
				KdPrint(("IoCreateSymbolicLink failed %X\n", status));
			}
		}
		else
			KdPrint(("IoCreateDevice failed %X\n", status));
	}

	ExReleaseFastMutexUnsafe(&ControlMutex);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CreateControlDevice() failed with %X\n", status));

	return status;
}

VOID DeleteControlObject ()
{
	KdPrint(("Entering DeleteControlObject\n"));

	ExAcquireFastMutexUnsafe(&ControlMutex);

	// If this is the last instance of the device then delete the controlobject
	// and symbolic link to enable the pnp manager to unload the driver.
	if (!(--InstanceCount) && ControlDeviceObject)
	{
		UNICODE_STRING symbolicLinkName;
		RtlInitUnicodeString(&symbolicLinkName, CONTROL_LINK);
		IoDeleteSymbolicLink(&symbolicLinkName);

		IoDeleteDevice(ControlDeviceObject);
		ControlDeviceObject = NULL;
		KdPrint(("ControlDeviceObject is now NULL\n"));

		UnpatchCommandTable();
	}

	ExReleaseFastMutexUnsafe(&ControlMutex);

}

#pragma prefast(suppress:28152, "We don't need to clear DO_DEVICE_INITIALIZING if fido isn't created")
NTSTATUS AddDevice (IN PDRIVER_OBJECT DriverObject, IN PDEVICE_OBJECT pdo)
{
	KdPrint((DRIVERNAME " - Entering AddDevice: DriverObject %p, pdo %p\n", DriverObject, pdo));

	UNICODE_STRING pdoname;
	RtlInitUnicodeString(&pdoname, L"\\Driver\\Fdc");

#pragma prefast(suppress:28175, "For safe filtering we need to check DriverName here")
	if (RtlCompareUnicodeString(&pdoname, &pdo->DriverObject->DriverName, TRUE))
	{
#pragma prefast(suppress:28175, "Accessing DriverName for logging")
		KdPrint(("Rejecting device for non-FDC driver: %wZ\n", &pdo->DriverObject->DriverName));
		return STATUS_SUCCESS;
	}

	KdPrint(("Patching FDC command table\n"));
	PatchCommandTable(pdo);


	NTSTATUS status;

	// Create a device object.	Do *not* specify any device characteristics here because
	// some of them will get applied to all drivers in the stack, and it's not up to
	// us as a filter to control any of the propagated attributes (e.g., FILE_DEVICE_SECURE_OPEN)

	PDEVICE_OBJECT fido;
	status = IoCreateDevice(DriverObject, sizeof(DEVICE_EXTENSION), NULL, FILE_DEVICE_UNKNOWN, 0, FALSE, &fido);
	if (!NT_SUCCESS(status))
	{
		KdPrint((DRIVERNAME " - IoCreateDevice failed - %X\n", status));
		return status;
	}

	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION) fido->DeviceExtension;
	pdx->IsEdo = FALSE;

	// From this point forward, any error will have side effects that need to be cleaned up

	do
	{
		IoInitializeRemoveLock(&pdx->RemoveLock, 0, 0, 0);
		pdx->DeviceObject = fido;
		pdx->Pdo = pdo;

		// Add our device object to the stack and propagate critical settings lower device object

		PDEVICE_OBJECT fdo = IoAttachDeviceToDeviceStack(fido, pdo);
		if (!fdo)
		{
			// Can't attach
			KdPrint((DRIVERNAME " - IoAttachDeviceToDeviceStack failed\n"));
			status = STATUS_DEVICE_REMOVED;
			break;
		}

		pdx->LowerDeviceObject = fdo;

		// Copy the flags related to I/O buffering from the lower device object so the I/O manager
		// will create the expected data structures for reads and writes.
		fido->Flags |= fdo->Flags & (DO_DIRECT_IO | DO_BUFFERED_IO | DO_POWER_PAGABLE);
		KdPrint(("fido->Flags = %X\n", fido->Flags));

		// Clear the "initializing" flag so that we can get IRPs
		fido->Flags &= ~DO_DEVICE_INITIALIZING;

		FDC_INFO fdci;
		RtlZeroMemory(&fdci, sizeof(fdci));
		status = FlFdcDeviceIo(pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_GET_FDC_INFO, &fdci);
		pdx->DeviceUnit = (UCHAR)fdci.PeripheralNumber;

		if (NT_SUCCESS(status))
		{
			KdPrint(("### GetFdcInfo:\n"));
			KdPrint((" FloppyControllerType = %x\n SpeedsAvailable = %x\n AdapterBufferSize = %lu\n", fdci.FloppyControllerType, fdci.SpeedsAvailable, fdci.AdapterBufferSize));
			KdPrint((" BusType = %x\n BusNumber = %lu\n ControllerNumber = %lu\n PeripheralNumber = %lu\n UnitNumber = %lu\n", fdci.BusType, fdci.BusNumber, fdci.ControllerNumber, fdci.PeripheralNumber, fdci.UnitNumber));
			KdPrint((" MaxTransferSize = %lu\n", fdci.MaxTransferSize));
			KdPrint((" AcpiBios = %lu\n AcpiFdiSupported = %lu\n", fdci.AcpiBios, fdci.AcpiFdiSupported));
			KdPrint((" BufferCount = %lu\n BufferSize = %lu\n BufferAddress = %p\n", fdci.BufferCount, fdci.BufferSize, fdci.BufferAddress));
		}

		WCHAR edobuf[32], linkbuf[32];
		_snwprintf(edobuf, arraysize(edobuf)-1, DEVICE_TEMPLATE, pdx->DeviceUnit);
		_snwprintf(linkbuf, arraysize(linkbuf)-1, LINK_TEMPLATE, pdx->DeviceUnit);

		UNICODE_STRING edoname, linkname;
		RtlInitUnicodeString(&edoname, edobuf);
		RtlInitUnicodeString(&linkname, linkbuf);

		PDEVICE_OBJECT edo;
#pragma prefast(suppress:6014, "edo is kept in the device extension, and is not a leak")
		status = IoCreateDevice(DriverObject, sizeof(EXTRA_DEVICE_EXTENSION), &edoname, FILE_DEVICE_UNKNOWN, 0, TRUE, &edo);
		if (!NT_SUCCESS(status))
		{
			KdPrint((DRIVERNAME " - Failed to create edo object\n"));
			break;
		}

		// IRP_MJ_READ/WRITE should use MDLs for data transfers
		edo->Flags |= DO_DIRECT_IO;

		PEXTRA_DEVICE_EXTENSION edx = (PEXTRA_DEVICE_EXTENSION)edo->DeviceExtension;
		edx->IsEdo = TRUE;
		edx->DeviceObject = edo;
		edx->pdx = pdx;
		pdx->edx = edx;

		KeInitializeSemaphore(&edx->semaphore, 0, 0xffff);
		edx->AcquireCount = 0;
		edx->MaxTransferSize = fdci.MaxTransferSize;

		edx->Status = STATUS_SUCCESS;
		KeInitializeSpinLock(&edx->IrpQueueLock);
		InitializeListHead(&edx->IrpQueueAnchor);
		status = IoCsqInitialize(&edx->IrpQueue, InsertIrp, RemoveIrp, PeekNextIrp, AcquireLock, ReleaseLock, CompleteCanceledIrp);
		if (!NT_SUCCESS(status))
		{
			KdPrint((DRIVERNAME " - Failed to init csq\n"));
			break;
		}

		status = IoCreateSymbolicLink(&linkname, &edoname);
		if (!NT_SUCCESS(status))
		{
			KdPrint((DRIVERNAME " - Failed to create symbolic link\n"));
			break;
		}

		edo->Flags &= ~DO_DEVICE_INITIALIZING;
	}
	while (FALSE);

	if (!NT_SUCCESS(status))
	{
		if (pdx->edx)
		{
#pragma prefast(suppress:28107, "pdx->edx->DeviceObject only exists and is safely only touched if pdx->edx is valid")
			IoDeleteDevice(pdx->edx->DeviceObject);
		}

		if (pdx->LowerDeviceObject)
			IoDetachDevice(pdx->LowerDeviceObject);

		IoDeleteDevice(fido);
	}

	return status;
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS CompleteRequest (IN PIRP Irp, IN NTSTATUS status, IN ULONG_PTR info/*=0*/, IN CCHAR boost/*=IO_NO_INCREMENT*/)
{
	Irp->IoStatus.Status = status;
	Irp->IoStatus.Information = NT_SUCCESS(status) ? info : 0;
	IoCompleteRequest(Irp, NT_SUCCESS(status) ? boost : IO_NO_INCREMENT);
	return status;
}

VOID Sleep (LONG lMilliseconds_)
{
	LARGE_INTEGER Delay;
	Delay.QuadPart = -(10*1000 * lMilliseconds_);	// time in ms
	KeDelayExecutionThread(KernelMode, FALSE, &Delay);
}

///////////////////////////////////////////////////////////////////////////////


NTSTATUS DispatchAny (IN PDEVICE_OBJECT fido, IN PIRP Irp)
{
	PIO_STACK_LOCATION stack = IoGetCurrentIrpStackLocation(Irp);
//	KdPrint(("DispatchAny(): PDEVICE_OBJECT fido=%#08lx, PIRP Irp=%#08lx [Major=%lu,Minor=%lu]\n", fido, Irp, stack->MajorFunction, stack->MinorFunction));

	// Catch IRPs for the Control Device Object (\\.\fdrawcmd)
	// A restarted device will prevent us recognising the old CDO, so also check if there's no device extension
	if (fido == ControlDeviceObject || !fido->DeviceExtension)
	{
		switch (stack->MajorFunction)
		{
			case IRP_MJ_CREATE:
				KdPrint(("CDO: IRP_MJ_CREATE\n"));
				return CompleteRequest(Irp, stack->FileObject->FileName.Length ? STATUS_OBJECT_PATH_NOT_FOUND : STATUS_SUCCESS);

			case IRP_MJ_CLOSE:
				KdPrint(("CDO: IRP_MJ_CLOSE\n"));
				return CompleteRequest(Irp, STATUS_SUCCESS);

			case IRP_MJ_CLEANUP:
				KdPrint(("CDO: IRP_MJ_CLEANUP\n"));
				return CompleteRequest(Irp, STATUS_SUCCESS);

			case IRP_MJ_DEVICE_CONTROL:
			{
				NTSTATUS status = STATUS_SUCCESS;

				if (stack->Parameters.DeviceIoControl.IoControlCode != IOCTL_FDRAWCMD_GET_VERSION)
				{
					KdPrint(("CDO: unhandled IoControlCode (%X)\n", stack->Parameters.DeviceIoControl.IoControlCode));
					status = STATUS_INVALID_DEVICE_REQUEST;
				}
				else if (stack->Parameters.DeviceIoControl.OutputBufferLength < sizeof(UINT32))
					status = STATUS_BUFFER_TOO_SMALL;
				else
				{
					KdPrint(("CDO: IOCTL_FDRAWCMD_GET_VERSION\n"));
					*(PUINT32)Irp->AssociatedIrp.SystemBuffer = DRIVER_VERSION;
				}

				return CompleteRequest(Irp, status, sizeof(UINT32));
			}

			default:
				KdPrint(("CDO: unhandled IRP (%X)\n", stack->MajorFunction));
				return CompleteRequest(Irp, STATUS_INVALID_DEVICE_REQUEST);
		}
	}

	// Catch IRPs for the Extra Device Object (\\.\fdrawX)
	PCOMMON_DEVICE_EXTENSION cdx = (PCOMMON_DEVICE_EXTENSION)fido->DeviceExtension;
	if (cdx->IsEdo)
	{
		PEXTRA_DEVICE_EXTENSION edx = (PEXTRA_DEVICE_EXTENSION)cdx;

		switch (stack->MajorFunction)
		{
			case IRP_MJ_CREATE:
			{
				KdPrint(("EDO: IRP_MJ_CREATE\n"));
				NTSTATUS status = edx->Status;

				// The extra path must be blank for now, as we'll make use of it for formats later
				if (NT_SUCCESS(status) && stack->FileObject->FileName.Length != 0)
				    status = STATUS_OBJECT_PATH_NOT_FOUND;

				if (NT_SUCCESS(status))
					status = AcquireFdc(edx, 4);

				return CompleteRequest(Irp, status);
			}

			case IRP_MJ_CLOSE:
				KdPrint(("EDO: IRP_MJ_CLOSE\n"));

				if (NT_SUCCESS(edx->Status))
					ReleaseFdc(edx);

				return CompleteRequest(Irp, STATUS_SUCCESS);

			case IRP_MJ_CLEANUP:
			{
				KdPrint(("EDO: IRP_MJ_CLEANUP\n"));
				SetDefaults(edx);

				return CompleteRequest(Irp, STATUS_SUCCESS);
			}

			case IRP_MJ_DEVICE_CONTROL:
			{
				NTSTATUS status = edx->Status;

				// If there's an outstanding error condition, fail with it
				if (!NT_SUCCESS(status))
					return CompleteRequest(Irp, status);

				IoCsqInsertIrp(&edx->IrpQueue, Irp, 0);
//				KdPrint(("EDO: queued IRP %X\n", Irp));
				KeReleaseSemaphore(&edx->semaphore, 0, 1, FALSE);
				return STATUS_PENDING;
			}

			default:
				KdPrint(("!!! EDO: unhandled IRP (%X)\n", stack->MajorFunction));
				return CompleteRequest(Irp, STATUS_INVALID_DEVICE_REQUEST);
		}
	}

	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION) fido->DeviceExtension;

	// Pass request down without additional processing

	NTSTATUS status;
	status = IoAcquireRemoveLock(&pdx->RemoveLock, Irp);
	if (!NT_SUCCESS(status))
		return CompleteRequest(Irp, status);
	IoSkipCurrentIrpStackLocation(Irp);
	status = IoCallDriver(pdx->LowerDeviceObject, Irp);
	IoReleaseRemoveLock(&pdx->RemoveLock, Irp);
	return status;
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS DispatchPower (IN PDEVICE_OBJECT fido, IN PIRP Irp)
{
	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION) fido->DeviceExtension;
	PoStartNextPowerIrp(Irp);	// must be done while we own the IRP

	NTSTATUS status;
	status = IoAcquireRemoveLock(&pdx->RemoveLock, Irp);
	if (!NT_SUCCESS(status))
		return CompleteRequest(Irp, status);

	IoSkipCurrentIrpStackLocation(Irp);

	status = PoCallDriver(pdx->LowerDeviceObject, Irp);
	IoReleaseRemoveLock(&pdx->RemoveLock, Irp);

	return status;
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS DispatchPnp (IN PDEVICE_OBJECT fido, IN PIRP Irp)
{
	PIO_STACK_LOCATION stack = IoGetCurrentIrpStackLocation(Irp);
	ULONG fcn = stack->MinorFunction;

	NTSTATUS status;
	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION) fido->DeviceExtension;
	status = IoAcquireRemoveLock(&pdx->RemoveLock, Irp);
	if (!NT_SUCCESS(status))
		return CompleteRequest(Irp, status);

	// Handle usage notification specially in order to track power pageable
	// flag correctly. We need to avoid allowing a non-pageable handler to be
	// layered on top of a pageable handler.

	if (fcn == IRP_MN_DEVICE_USAGE_NOTIFICATION)
	{
#pragma prefast(suppress:28175, "This is part of Walter Oney's template code")
		if (!fido->AttachedDevice || (fido->AttachedDevice->Flags & DO_POWER_PAGABLE))
			fido->Flags |= DO_POWER_PAGABLE;
		IoCopyCurrentIrpStackLocationToNext(Irp);
		IoSetCompletionRoutine(Irp, UsageNotificationCompletionRoutine, (PVOID) pdx, TRUE, TRUE, TRUE);
		return IoCallDriver(pdx->LowerDeviceObject, Irp);
	}

	// Handle start device specially in order to correctly inherit FILE_REMOVABLE_MEDIA

	if (fcn == IRP_MN_START_DEVICE)
	{
		KdPrint((DRIVERNAME " - In IRP_MN_START_DEVICE\n"));

		IoCopyCurrentIrpStackLocationToNext(Irp);
		IoSetCompletionRoutine(Irp, StartDeviceCompletionRoutine, (PVOID) pdx, TRUE, TRUE, TRUE);
		status = IoCallDriver(pdx->LowerDeviceObject, Irp);
		StartThread(pdx->edx);
		return status;
	}

	// Handle remove device specially in order to cleanup device stack

	if (fcn == IRP_MN_REMOVE_DEVICE)
	{
		KdPrint((DRIVERNAME " - In IRP_MN_REMOVE_DEVICE\n"));
		IoSkipCurrentIrpStackLocation(Irp);
		status = IoCallDriver(pdx->LowerDeviceObject, Irp);
		IoReleaseRemoveLockAndWait(&pdx->RemoveLock, Irp);

		DeleteControlObject();
		StopThread(pdx->edx);
		RemoveDevice(fido);

		return status;
	}

	// Simply forward any other type of PnP request

	IoSkipCurrentIrpStackLocation(Irp);
	status = IoCallDriver(pdx->LowerDeviceObject, Irp);
	IoReleaseRemoveLock(&pdx->RemoveLock, Irp);
	return status;
}

///////////////////////////////////////////////////////////////////////////////

VOID RemoveDevice (IN PDEVICE_OBJECT fido)
{
	KdPrint((DRIVERNAME " - In RemoveDevice\n"));
	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION) fido->DeviceExtension;

	WCHAR buf[32];
	_snwprintf(buf, arraysize(buf)-1, LINK_TEMPLATE, pdx->DeviceUnit);
	UNICODE_STRING link;
	RtlInitUnicodeString(&link, buf);
	IoDeleteSymbolicLink(&link);

	for (PIRP Irp ; (Irp = IoCsqRemoveNextIrp(&pdx->edx->IrpQueue, 0)) ; CompleteRequest(Irp, pdx->edx->Status))
		KdPrint((DRIVERNAME " - Cancelling IRP (%p)\n", Irp));

	IoDeleteDevice(pdx->edx->DeviceObject);

	if (pdx->LowerDeviceObject)
	{
		IoDetachDevice(pdx->LowerDeviceObject);
		pdx->LowerDeviceObject = NULL;
	}

	IoDeleteDevice(fido);
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS StartDeviceCompletionRoutine (PDEVICE_OBJECT fido, PIRP Irp, PVOID context)
{
	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION)context;

	if (Irp->PendingReturned)
		IoMarkIrpPending(Irp);

	// Inherit FILE_REMOVABLE_MEDIA flag from lower object. This is necessary
	// for a disk filter, but it isn't available until start-device time. Drivers
	// above us may examine the flag as part of their own start-device processing, too.

	if (pdx->LowerDeviceObject->Characteristics & FILE_REMOVABLE_MEDIA)
		fido->Characteristics |= FILE_REMOVABLE_MEDIA;

	CreateControlObject(fido);

	IoReleaseRemoveLock(&pdx->RemoveLock, Irp);
	return STATUS_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS UsageNotificationCompletionRoutine(PDEVICE_OBJECT fido, PIRP Irp, PVOID context)
{
	PDEVICE_EXTENSION pdx = (PDEVICE_EXTENSION)context;

	if (Irp->PendingReturned)
		IoMarkIrpPending(Irp);

	// If lower driver cleared pageable flag, we must do the same

	if (!(pdx->LowerDeviceObject->Flags & DO_POWER_PAGABLE))
		fido->Flags &= ~DO_POWER_PAGABLE;

	IoReleaseRemoveLock(&pdx->RemoveLock, Irp);
	return STATUS_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS StartThread (PEXTRA_DEVICE_EXTENSION edx)
{
	NTSTATUS status;

	OBJECT_ATTRIBUTES oa;
	InitializeObjectAttributes(&oa, NULL, OBJ_KERNEL_HANDLE, NULL, NULL);

	edx->Status = STATUS_SUCCESS;

	HANDLE hthread;
	status = PsCreateSystemThread(&hthread, THREAD_ALL_ACCESS, &oa, NULL, NULL, ThreadProc, edx);
	if (!NT_SUCCESS(status))
		return status;

#pragma prefast(suppress:8126 309, "We have no Irp here, and the access mode is always KernelMode")
	ObReferenceObjectByHandle(hthread, THREAD_ALL_ACCESS, NULL, KernelMode, (PVOID*)&edx->thread, NULL);
	ZwClose(hthread);

	return STATUS_SUCCESS;
}

VOID StopThread (PEXTRA_DEVICE_EXTENSION edx)
{
	KdPrint(("Stopping thread\n"));
	edx->Status = STATUS_DELETE_PENDING;
	KeReleaseSemaphore(&edx->semaphore, 0, 1, FALSE);
	KeWaitForSingleObject(edx->thread, Executive, KernelMode, FALSE, NULL);
	ObDereferenceObject(edx->thread);
	edx->thread = NULL;
	KdPrint(("Thread stopped\n"));
}

NTSTATUS StartDmaThread (PEXTRA_DEVICE_EXTENSION edx, PKSTART_ROUTINE DmaThreadProc)
{
	// If we haven't got it, find the DMA adapter object pointer in the FDC extension
	if (!edx->ppAdapterObject && !(edx->ppAdapterObject = FindAdapterObject(edx->pdx->Pdo)))
		return STATUS_UNSUCCESSFUL;

	edx->StopDmaThread = FALSE;
	KeInitializeEvent(&edx->dmasync, SynchronizationEvent, FALSE);

	OBJECT_ATTRIBUTES oa;
	InitializeObjectAttributes(&oa, NULL, OBJ_KERNEL_HANDLE, NULL, NULL);

	HANDLE hthread;
	NTSTATUS status = PsCreateSystemThread(&hthread, THREAD_ALL_ACCESS, &oa, NULL, NULL, DmaThreadProc, edx);

	if (NT_SUCCESS(status))
	{
#pragma prefast(suppress:8126 309, "We have no Irp here, and the access mode is always KernelMode")
		ObReferenceObjectByHandle(hthread, THREAD_ALL_ACCESS, NULL, KernelMode, (PVOID*)&edx->dmathread, NULL);
		ZwClose(hthread);
	}

	return status;
}

VOID StopDmaThread (PEXTRA_DEVICE_EXTENSION edx)
{
	KdPrint(("Stopping DMA thread\n"));
	edx->StopDmaThread = TRUE;
	KeWaitForSingleObject(edx->dmathread, Executive, KernelMode, FALSE, NULL);
	ObDereferenceObject(edx->dmathread);
	edx->dmathread = NULL;
	KdPrint(("DMA thread stopped\n"));
}

///////////////////////////////////////////////////////////////////////////////

VOID FlFreeIoBuffer (IN OUT PEXTRA_DEVICE_EXTENSION edx)
{
	if (!edx->IoBuffer)
		return;

	edx->IoBufferSize = 0;

	MmUnlockPages(edx->IoBufferMdl);
	IoFreeMdl(edx->IoBufferMdl);
	edx->IoBufferMdl = NULL;

	ExFreePool(edx->IoBuffer);
	edx->IoBuffer = NULL;
}

VOID FlAllocateIoBuffer (IN OUT PEXTRA_DEVICE_EXTENSION edx, IN ULONG BufferSize)
{
	if (edx->IoBuffer)
	{
		if (edx->IoBufferSize >= BufferSize)
			return;

		FlFreeIoBuffer(edx);
	}

	edx->IoBuffer = (PUCHAR)ExAllocatePoolWithTag(NonPagedPoolCacheAligned, BufferSize, POOL_TAG);

	if (!edx->IoBuffer)
		return;

	edx->IoBufferMdl = IoAllocateMdl(edx->IoBuffer, BufferSize, FALSE, FALSE, NULL);

	if (!edx->IoBufferMdl)
	{
		ExFreePool(edx->IoBuffer);
		edx->IoBuffer = NULL;
		return;
	}

	__try
	{
		MmProbeAndLockPages(edx->IoBufferMdl, KernelMode, IoModifyAccess);
	}
	__except (GetExceptionCode()==STATUS_ACCESS_VIOLATION)
	{
		 KdPrint((DRIVERNAME " - MmProbeAndLockPages failed. Status = %X\n", GetExceptionCode()));

		 ExFreePool(edx->IoBuffer);
		 edx->IoBuffer = NULL;
		 return;
	}

	edx->IoBufferSize = BufferSize;
}


///////////////////////////////////////////////////////////////////////////////

NTSTATUS CheckBuffers (PIRP Irp, ULONG uInput_, ULONG uOutput_, bool fOutOptional_=false)
{
	PIO_STACK_LOCATION stack = IoGetCurrentIrpStackLocation(Irp);
	ULONG InSize  = stack->Parameters.DeviceIoControl.InputBufferLength;
	ULONG OutSize = stack->Parameters.DeviceIoControl.OutputBufferLength;

	if (InSize < uInput_)
		return STATUS_INVALID_PARAMETER;
	else if (fOutOptional_ && !OutSize)
		return STATUS_SUCCESS;
	else if (OutSize < uOutput_)
		return STATUS_BUFFER_TOO_SMALL;
	else
		return STATUS_SUCCESS;
}

void CopyOutput (PEXTRA_DEVICE_EXTENSION edx, PIRP Irp, ULONG uOutSize_)
{
	ULONG OutSize = IoGetCurrentIrpStackLocation(Irp)->Parameters.DeviceIoControl.OutputBufferLength;

	if (Irp->IoStatus.Information = (OutSize ? uOutSize_ : 0))
		RtlCopyMemory(Irp->AssociatedIrp.SystemBuffer, edx->FifoOut, uOutSize_);
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS CheckFdcResult (PEXTRA_DEVICE_EXTENSION edx)
{
	UCHAR status0 = edx->FifoOut[0], status1 = edx->FifoOut[1], status2 = edx->FifoOut[2];

	if ((status0 & STREG0_END_MASK) == STREG0_END_NORMAL)
		return STATUS_SUCCESS;					// ERROR_SUCCESS

	if ((status1 & STREG1_CRC_ERROR ) || (status2 & STREG2_CRC_ERROR))
		return STATUS_CRC_ERROR;				// ERROR_CRC

	if (status1 & STREG1_DATA_OVERRUN)
		return STATUS_DATA_OVERRUN; 			// ERROR_IO_DEVICE

	if ((status1 & STREG1_SECTOR_NOT_FOUND) || (status1 & STREG1_END_OF_DISKETTE))
		return STATUS_NONEXISTENT_SECTOR;		// ERROR_SECTOR_NOT_FOUND

	if ((status2 & STREG2_BAD_CYLINDER))
		return STATUS_FLOPPY_WRONG_CYLINDER;	// ERROR_FLOPPY_WRONG_CYLINDER

	if (status2 & STREG2_DATA_NOT_FOUND)
		return STATUS_NONEXISTENT_SECTOR;		// ERROR_SECTOR_NOT_FOUND

	if (status1 & STREG1_WRITE_PROTECTED)
		return STATUS_MEDIA_WRITE_PROTECTED;	// ERROR_WRITE_PROTECT

	if (status1 & STREG1_ID_NOT_FOUND)
		return STATUS_FLOPPY_ID_MARK_NOT_FOUND; // ERROR_FLOPPY_ID_MARK_NOT_FOUND

	if (status2 & STREG2_WRONG_CYLINDER)
		return STATUS_FLOPPY_WRONG_CYLINDER;	// ERROR_FLOPPY_WRONG_CYLINDER

	KdPrint(("### Unknown FDC error [%02X %02X %02X]\n", status0, status1, status2));

	edx->NeedReset = TRUE;
	return STATUS_FLOPPY_UNKNOWN_ERROR;
}

///////////////////////////////////////////////////////////////////////////////


NTSTATUS FlFdcDeviceIo (IN PDEVICE_OBJECT DeviceObject, IN ULONG Ioctl, IN OUT PVOID Data=NULL)
{
	NTSTATUS status;
	PIO_STACK_LOCATION irpStack;

	KEVENT doneEvent;
	KeInitializeEvent(&doneEvent, NotificationEvent, FALSE);

	// Create an IRP for enabler
	IO_STATUS_BLOCK ioStatus;
	PIRP irp = IoBuildDeviceIoControlRequest(Ioctl, DeviceObject, NULL, 0, NULL, 0, TRUE, &doneEvent, &ioStatus);

	if (!irp)
	{
		KdPrint((DRIVERNAME " - FlFdcDeviceIo: Can't allocate Irp\n"));
		// If an Irp can't be allocated, then this call will simply return.
		// This will leave the queue frozen for this device, which means it can no longer be accessed.
		return STATUS_INSUFFICIENT_RESOURCES;
	}

	irpStack = IoGetNextIrpStackLocation(irp);
	irpStack->Parameters.DeviceIoControl.Type3InputBuffer = Data;

	// Call the driver and request the operation
	status = IoCallDriver(DeviceObject, irp);

	if (status == STATUS_PENDING)
	{
		// Now wait for operation to complete (should already be done, but maybe not)
		KeWaitForSingleObject(&doneEvent, Executive, KernelMode, FALSE, NULL);
		status = ioStatus.Status;
	}

	return status;
}


NTSTATUS AcquireFdc (PEXTRA_DEVICE_EXTENSION edx, ULONG Seconds)
{
	if (InterlockedIncrement(&edx->AcquireCount) != 1)
	{
		KdPrint(("AcquireFdc: controller usage now %ld\n", edx->AcquireCount));
		return STATUS_SUCCESS;
	}

	LARGE_INTEGER wait;
	wait.QuadPart = SECONDS(Seconds);
	KdPrint(("AcquireFdc: controller usage now 1, acquiring controller.\n"));
	NTSTATUS status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_ACQUIRE_FDC, &wait);

	if (NT_SUCCESS(status))
		KdPrint((" controller acquired successfully\n"));
	else
	{
		InterlockedDecrement(&edx->AcquireCount);
		KdPrint(("!!! AcquireFdc failed with %X\n", status));
	}

	return status;
}

NTSTATUS ReleaseFdc (PEXTRA_DEVICE_EXTENSION edx)
{
	if (InterlockedDecrement(&edx->AcquireCount))
	{
		KdPrint(("ReleaseFdc: controller usage now %ld\n", edx->AcquireCount));
		return STATUS_SUCCESS;
	}

	KdPrint(("ReleaseFdc: controller usage now 0, releasing controller.\n"));
	NTSTATUS status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_RELEASE_FDC, edx->DeviceObject);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! ReleaseFdc failed with %X\n", status));

	return STATUS_SUCCESS;
}

NTSTATUS EnableFdc (PEXTRA_DEVICE_EXTENSION edx, USHORT MotorSettleTime)
{
	NTSTATUS status;

	FDC_ENABLE_PARMS fep;
	fep.DriveOnValue = edx->DriveOnValue;
	fep.TimeToWait = MotorSettleTime;

	status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_ENABLE_FDC_DEVICE, &fep);
	edx->Enabled = TRUE;

	if (!NT_SUCCESS(status))
		KdPrint(("!!! EnableFdc failed with %X\n", status));

	return status;
}

NTSTATUS DisableFdc (PEXTRA_DEVICE_EXTENSION edx)
{
	NTSTATUS status;

	status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_DISABLE_FDC_DEVICE, NULL);
	edx->Enabled = FALSE;

	// Restore the single motor drive-on value for next time
	edx->DriveOnValue = (FDC_MOTOR_A << edx->DeviceUnit) | (FDC_SELECT_A + edx->DeviceUnit);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! DisableFdc failed with %X\n", status));

	return status;
}

NTSTATUS SetFdcDataRate (PEXTRA_DEVICE_EXTENSION edx, UCHAR rate)
{
	KdPrint(("SetFdcDataRate(%u)\n", rate));

	if (rate > 3)							// 0=500Kbps, 1=300Kbps, 2=250Kbps (default), 3=1Mbps
		return STATUS_INVALID_PARAMETER;

	NTSTATUS status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_SET_FDC_DATA_RATE, &rate);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! SetFdcDataRate failed with %X\n", status));

	return status;
}

NTSTATUS GetFdcDiskChange (PEXTRA_DEVICE_EXTENSION edx, PUCHAR pstatus)
{
	FDC_DISK_CHANGE_PARMS fdcp;
	fdcp.DriveStatus = 0x00;
	fdcp.DriveOnValue = edx->DriveOnValue;

	KdPrint(("Issuing IOCTL_DISK_INTERNAL_GET_FDC_DISK_CHANGE with %02X %02X (size is %lu)\n", fdcp.DriveStatus, fdcp.DriveOnValue, sizeof(FDC_DISK_CHANGE_PARMS)));
	NTSTATUS status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_GET_FDC_DISK_CHANGE, &fdcp);

	if (NT_SUCCESS(status) && pstatus)
		*pstatus = fdcp.DriveStatus;

	if (!NT_SUCCESS(status))
		KdPrint(("!!! GetFdcDiskChange failed with %X\n", status));

	return status;
}

NTSTATUS GetFdcInfo (PEXTRA_DEVICE_EXTENSION edx, PFDC_INFO pfdci)
{
	NTSTATUS status;

	RtlZeroMemory(pfdci, sizeof(FDC_INFO));
	pfdci->BufferCount = 0;
	pfdci->BufferSize = 0;
	status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_GET_FDC_INFO, pfdci);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! GetFdcInfo failed with %X\n", status));

	return status;
}

NTSTATUS ResetFdc (PEXTRA_DEVICE_EXTENSION edx)
{
	NTSTATUS status;

	KdPrint(("*** Resetting FDC\n"));

	status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_RESET_FDC);

	if (NT_SUCCESS(status))
	{
		KdPrint(("!!! ResetFdc failed with %X, retrying in 500ms...\n", status));
	    Sleep(500);
		status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_RESET_FDC);
	}

	return status;
}

///////////////////////////////////////////////////////////////////////////////


NTSTATUS FlIssueCommand (IN OUT PEXTRA_DEVICE_EXTENSION edx, IN PMDL MdlAddress_=NULL, IN ULONG MdlSize_=0)
{
	RtlZeroMemory(edx->FifoOut, sizeof(edx->FifoOut));

	//	Set the command parameters
	ISSUE_FDC_COMMAND_PARMS issueCommandParms;
	issueCommandParms.FifoInBuffer = edx->FifoIn;
	issueCommandParms.FifoOutBuffer = edx->FifoOut;
	issueCommandParms.IoHandle = MdlAddress_;
	issueCommandParms.IoOffset = 0;
	issueCommandParms.TransferBytes = (MdlAddress_ && !MdlSize_) ? MmGetMdlByteCount(MdlAddress_) : MdlSize_;
	issueCommandParms.TimeOut = FDC_TIMEOUT;

	// Cap the request size at the DMA transfer limit
	if (issueCommandParms.TransferBytes > edx->MaxTransferSize)
		issueCommandParms.TransferBytes = edx->MaxTransferSize;

//	KdPrint((DRIVERNAME " FlFdcDeviceIo (cmd %x (%x), %u bytes, %u timeout)\n", edx->FifoIn[0], (edx->FifoIn[0]&0x1f), issueCommandParms.TransferBytes, issueCommandParms.TimeOut));
	NTSTATUS status = FlFdcDeviceIo(edx->pdx->LowerDeviceObject, IOCTL_DISK_INTERNAL_ISSUE_FDC_COMMAND, &issueCommandParms);
//	KdPrint((" returned %X\n", status));

	if (status == STATUS_DEVICE_NOT_READY || status == STATUS_FLOPPY_BAD_REGISTERS)
	{
		KdPrint(("!!! FlIssueCommand returned %X, requesting reset\n", status));
		edx->NeedReset = TRUE;
	}

	return status;
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS CommandVersion (PEXTRA_DEVICE_EXTENSION edx)
{
	edx->FifoIn[0] = COMMND_VERSION;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandVersion failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandPartId (PEXTRA_DEVICE_EXTENSION edx)
{
	edx->FifoIn[0] = COMMND_PART_ID;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandPartId failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandPerpendicularMode (PEXTRA_DEVICE_EXTENSION edx, UCHAR flags)
{
	flags &= (0xc3 | (0x04 << edx->DeviceUnit));		// allow only current drive bit

	edx->FifoIn[0] = COMMND_PERPENDICULAR_MODE;
	edx->FifoIn[1] = flags;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandPerpendicularMode failed with %X [%02X %02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandLock (PEXTRA_DEVICE_EXTENSION edx, UCHAR lock)
{
	lock &= 0x80;

	edx->FifoIn[0] = lock | COMMND_LOCK;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandLock failed with %X [%02X %02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}


NTSTATUS CommandDumpReg (PEXTRA_DEVICE_EXTENSION edx)
{
	edx->FifoIn[0] = COMMND_DUMPREG;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandDumpReg failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandSeek (PEXTRA_DEVICE_EXTENSION edx, UCHAR head, UCHAR cyl)
{
	// Old drives require a delay to avoid missed seeks
	if (!edx->NeedDisk)
		Sleep(edx->HeadSettleTime);

	head &= 1;

	edx->FifoIn[0] = COMMND_SEEK;
	edx->FifoIn[1] = (head << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = cyl;
	NTSTATUS status = FlIssueCommand(edx);

	if (NT_SUCCESS(status))
	{
		if (!edx->NeedDisk && (edx->FifoOut[0] & STREG0_SEEK_COMPLETE) && (edx->FifoOut[1] == cyl))
		{
			// *** This delay causes us to miss the start of the next track on SAM disks with a skew of 1 ***
			Sleep(edx->HeadSettleTime);
		}

		edx->PhysCyl = edx->FifoOut[1];
	}

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandSeek failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));
	else
		KdPrint(("CommandSeek: [%02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandRelativeSeek (PEXTRA_DEVICE_EXTENSION edx, UCHAR flags, UCHAR head, UCHAR offset)
{
	// Old drives require a delay to avoid missed seeks
	if (!edx->NeedDisk)
		Sleep(edx->HeadSettleTime);

	flags &= COMMND_OPTION_DIRECTION;
	head &= 1;

	edx->FifoIn[0] = flags | COMMND_RELATIVE_SEEK;
	edx->FifoIn[1] = (head << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = offset;
	NTSTATUS status = FlIssueCommand(edx);

	if (NT_SUCCESS(status) && !edx->NeedDisk)
	{
		if (!edx->NeedDisk && (edx->FifoOut[0] & STREG0_SEEK_COMPLETE))
			Sleep(edx->HeadSettleTime); 	// time in ms

		edx->PhysCyl = edx->FifoOut[1];
	}

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandRelativeSeek failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));
	else
		KdPrint(("CommandRelativeSeek: [%02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandReadId (PEXTRA_DEVICE_EXTENSION edx, UCHAR flags, UCHAR phead)
{
	flags &= COMMND_OPTION_MFM;
	phead &= 1;

	edx->FifoIn[0] = flags | COMMND_READ_ID;
	edx->FifoIn[1] = (phead << 2) | edx->DeviceUnit;
	NTSTATUS status = FlIssueCommand(edx);

	if (NT_SUCCESS(status))
		status = CheckFdcResult(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandReadId failed with %X [ST0:%02X ST1:%02X ST2:%02X  C:%02X H:%02X R:%02X N:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));
	else
		KdPrint(("CommandReadId: [%02X %02X %02X  %02X %02X %02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));

	return status;
}

NTSTATUS CommandRecalibrate (PEXTRA_DEVICE_EXTENSION edx)
{
	// Old drives require a delay to avoid missed seeks
	if (!edx->NeedDisk)
		Sleep(edx->HeadSettleTime);

	edx->FifoIn[0] = COMMND_RECALIBRATE;
	edx->FifoIn[1] = edx->DeviceUnit;
	FlIssueCommand(edx);
	NTSTATUS status = FlIssueCommand(edx);	// perform a second recalibrate to ensure we're home

	if (NT_SUCCESS(status))
	{
		Sleep(edx->HeadSettleTime);
		edx->PhysCyl = 0;
	}

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandRecalibrate failed with %X [%02X %02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));
	else
		KdPrint(("CommandRecalibrate: [%02X %02X %02X  %02X %02X %02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));


	return status;
}

NTSTATUS CommandReadWrite (PEXTRA_DEVICE_EXTENSION edx, PMDL Mdl, ULONG MdlSize, UCHAR command, UCHAR flags, UCHAR phead, UCHAR cyl, UCHAR head, UCHAR sector, UCHAR size, UCHAR eot, UCHAR gap, UCHAR datalen)
{
	flags &= (COMMND_OPTION_MFM | COMMND_OPTION_SKIP);	// strip COMMND_OPTION_MULTI_TRACK for now
	phead &= 1;

	edx->FifoIn[0] = flags | command;
	edx->FifoIn[1] = (phead << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = cyl;
	edx->FifoIn[3] = head;
	edx->FifoIn[4] = sector;
	edx->FifoIn[5] = size;
	edx->FifoIn[6] = eot;
	edx->FifoIn[7] = gap;
	edx->FifoIn[8] = datalen;
	KdPrint((DRIVERNAME " CommandReadWrite: %02X  DS:%02X C:%02X H:%02X R:%02X N:%02X EOT:%02X GPL:%02X DTL:%02X\n", edx->FifoIn[0], edx->FifoIn[1], edx->FifoIn[2], edx->FifoIn[3], edx->FifoIn[4], edx->FifoIn[5], edx->FifoIn[6], edx->FifoIn[7], edx->FifoIn[8]));
	NTSTATUS status = FlIssueCommand(edx, Mdl, MdlSize);

	if (NT_SUCCESS(status))
		status = CheckFdcResult(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandReadWrite failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));

	return status;
}

NTSTATUS CommandVerify (PEXTRA_DEVICE_EXTENSION edx, UCHAR flags, UCHAR phead, UCHAR cyl, UCHAR head, UCHAR sector, UCHAR size, UCHAR eot, UCHAR gap, UCHAR datalen)
{
	UCHAR flags2 = (flags & 1) << 7;						// EC bit
	flags &= (COMMND_OPTION_MFM | COMMND_OPTION_SKIP);		// strip COMMND_OPTION_MULTI_TRACK for now
	phead &= 1;

	KdPrint(("CommandVerify: flags=%X, flags2=%X phead=%u, cyl=%u, head=%u, sector=%u, size=%u, count=%u, gap=%u, datalen=%u\n",
				flags, flags2, phead, cyl, head, sector, size, eot, gap, datalen));

	edx->FifoIn[0] = flags | COMMND_VERIFY;
	edx->FifoIn[1] = flags2 | (phead << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = cyl;
	edx->FifoIn[3] = head;
	edx->FifoIn[4] = sector;
	edx->FifoIn[5] = size;
	edx->FifoIn[6] = eot;
	edx->FifoIn[7] = gap;
	edx->FifoIn[8] = datalen;
	NTSTATUS status = FlIssueCommand(edx);

	if (NT_SUCCESS(status))
		status = CheckFdcResult(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandVerify failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));
	else
		KdPrint(("CommandVerify: [%02X %02X %02X  %02X %02X %02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));

	return status;
}

NTSTATUS CommandSenseDriveStatus (PEXTRA_DEVICE_EXTENSION edx, UCHAR bPhysHead_)
{
	bPhysHead_ &= 1;

	edx->FifoIn[0] = COMMND_SENSE_DRIVE_STATUS;
	edx->FifoIn[1] = (bPhysHead_ << 2) | edx->DeviceUnit;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandSenseDriveStatus failed with %X [ST3:%02X]\n", status, edx->FifoOut[0]));

	return status;
}

NTSTATUS CommandSenseInterruptStatus (PEXTRA_DEVICE_EXTENSION edx)
{
	edx->FifoIn[0] = COMMND_SENSE_INTERRUPT_STATUS;
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandSenseInterruptStatus failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));
	else
		KdPrint(("CommandSenseInterruptStatus: [ST0:%02X PVN:%02X]\n", edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandSpecify (PEXTRA_DEVICE_EXTENSION edx, UCHAR srt_hut, UCHAR hlt_nd)
{
	hlt_nd &= ~1;	// FDC.SYS relies on DMA, so strip the non-DMA bit

	edx->FifoIn[0] = COMMND_SPECIFY;
	edx->FifoIn[1] = srt_hut;	// typically 0xdf
	edx->FifoIn[2] = hlt_nd;	// typically 0x02 (bit 0 reset for dma mode)
	KdPrint(("Issuing SPECIFY with  %02X  %02X %02X\n", edx->FifoIn[0], edx->FifoIn[1], edx->FifoIn[2]));
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandSpecify failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandConfigure (PEXTRA_DEVICE_EXTENSION edx, UCHAR flags, UCHAR pretrk)
{
	edx->FifoIn[0] = COMMND_CONFIGURE;	// fdc.sys will set COMMND_OPTION_CLK48 if needed
	edx->FifoIn[1] = 0; 				// always zero
	edx->FifoIn[2] = flags;
	edx->FifoIn[3] = pretrk;

	KdPrint(("Issuing CONFIGURE with  %02X  %02X %02X %02X\n", edx->FifoIn[0], edx->FifoIn[1], edx->FifoIn[2], edx->FifoIn[3]));
	NTSTATUS status = FlIssueCommand(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandConfigure failed with %X [ST0:%02X C:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1]));

	return status;
}

NTSTATUS CommandReadTrack (PEXTRA_DEVICE_EXTENSION edx, PMDL Mdl, ULONG MdlSize, UCHAR flags, UCHAR phead, UCHAR cyl, UCHAR head, UCHAR sector, UCHAR size, UCHAR eot, UCHAR gap, UCHAR datalen)
{
	NTSTATUS status;

	flags &= COMMND_OPTION_MFM;
	phead &= 1;

	edx->FifoIn[0] = flags | COMMND_READ_TRACK;
	edx->FifoIn[1] = (phead << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = cyl;
	edx->FifoIn[3] = head;
	edx->FifoIn[4] = sector;
	edx->FifoIn[5] = size;
	edx->FifoIn[6] = eot;
	edx->FifoIn[7] = gap;
	edx->FifoIn[8] = datalen;
	KdPrint((DRIVERNAME " CommandReadTrack: %02X  DS:%02X C:%02X H:%02X R:%02X N:%02X EOT:%02X GPL:%02X DTL:%02X\n", edx->FifoIn[0], edx->FifoIn[1], edx->FifoIn[2], edx->FifoIn[3], edx->FifoIn[4], edx->FifoIn[5], edx->FifoIn[6], edx->FifoIn[7], edx->FifoIn[8]));
	status = FlIssueCommand(edx, Mdl, MdlSize);

	if (NT_SUCCESS(status))
		status = CheckFdcResult(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandReadTrack failed with %X [ST0:%02X ST1:%02X ST2:%02X  C:%02X H:%02X R:%02X N:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));
	else
		KdPrint(("CommandReadTrack: [%02X %02X %02X  %02X %02X %02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));

	return status;
}

NTSTATUS CommandFormatTrack (PEXTRA_DEVICE_EXTENSION edx, PMDL Mdl, ULONG MdlSize, UCHAR flags, UCHAR phead, UCHAR size, UCHAR sectors, UCHAR gap, UCHAR fill)
{
	flags &= COMMND_OPTION_MFM;
	phead &= 1;

	edx->FifoIn[0] = flags | COMMND_FORMAT_TRACK;
	edx->FifoIn[1] = (phead << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = size;
	edx->FifoIn[3] = sectors;
	edx->FifoIn[4] = gap;
	edx->FifoIn[5] = fill;
	NTSTATUS status = FlIssueCommand(edx, Mdl, MdlSize);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandFormatTrack failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));
	else
		KdPrint(("CommandFormatTrack: [%02X %02X %02X  %02X %02X %02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));

	return status;
}

NTSTATUS CommandFormatAndWrite (PEXTRA_DEVICE_EXTENSION edx, PMDL Mdl, ULONG MdlSize, UCHAR flags, UCHAR phead, UCHAR size, UCHAR sectors, UCHAR gap)
{
	flags &= COMMND_OPTION_MFM;
	phead &= 1;

	edx->FifoIn[0] = flags | COMMND_FORMAT_AND_WRITE;
	edx->FifoIn[1] = (phead << 2) | edx->DeviceUnit;
	edx->FifoIn[2] = size;
	edx->FifoIn[3] = sectors;
	edx->FifoIn[4] = gap;
	edx->FifoIn[5] = 0x00;	// fill not used
	NTSTATUS status = FlIssueCommand(edx, Mdl, MdlSize);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandFormatAndWrite failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));
	else
		KdPrint(("CommandFormatAndWrite: [%02X %02X %02X  %02X %02X %02X %02X]\n", edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2], edx->FifoOut[3], edx->FifoOut[4], edx->FifoOut[5], edx->FifoOut[6]));

	return status;
}

///////////////////////////////////////////////////////////////////////////////


NTSTATUS SendRawCommand (PUCHAR pb_, ULONG ul_)
{
	KdPrint(("SendRawCommand: writing %lu byte command: ", ul_));
	for (ULONG b = 0 ; b < ul_ ; b++) { KdPrint(("%02X ", pb_[b])); }
	KdPrint(("\n"));

	ULONG u = 0;

	while (1)
	{
		UCHAR bStat = READ_PORT_UCHAR(FDC_MSR_PORT);

		// If RQM is clear, we can't transfer any data
		if (!(bStat & 0x80))
			Sleep(10);

		// If DIO is set, the device is sending us data!
		else if (bStat & 0x40)
		{
			KdPrint(("!!! device is sending us data!\n"));
			return STATUS_DEVICE_NOT_READY;
		}

		// If we're about to send the first byte, ensure it's not already busy
		else if (!u && bStat & 0x10)
		{
			KdPrint(("!!! device is busy with an existing command!\n"));
			return STATUS_DEVICE_NOT_READY;
		}

		// Device is ready to accept a command byte
		else
		{
			UCHAR b = pb_[u++];

			WRITE_PORT_UCHAR(FDC_FIFO_PORT, b);
			KdPrint((" -> %02X\n", b));
			KeStallExecutionProcessor(10);

			// Sent everything?
			if (u >= ul_)
				break;
		}
	}

	KdPrint(("SendRawCommand: finished sending command\n"));
	return STATUS_SUCCESS;
}

NTSTATUS GetRawResult (PUCHAR pb_, ULONG ul_)
{
	KdPrint(("GetRawResult: reading command result\n"));

	ULONG u = 0;

	while (1)
	{
		UCHAR bStat = READ_PORT_UCHAR(FDC_MSR_PORT);

		// If CMD BUSY is clear, no command is in progress so no results are expected
		if (!(bStat & 0x10))
			break;

		// If RQM is clear, we can't transfer any data
		if (!(bStat & 0x80))
			Sleep(10);

		// If NON DMA is set, this is a command data transfer and not results data
		else if (bStat & 0x20)
		{
			KdPrint(("!!! device is providing non-results data!\n"));
			break;
		}

		// If DIO is clear, the device is waiting for us to send data
		else if (!(bStat & 0x40))
		{
			KdPrint(("!!! device is expecting data!\n"));
			break;
		}

		// A results byte is available, so read it
		else
		{
			UCHAR b = READ_PORT_UCHAR(FDC_FIFO_PORT);

			if (u < ul_)
			{
				pb_[u++] = b;
				KdPrint((" <- %02X\n", b));
			}
			else
			{
				KdPrint((" <- %02X (discarded)\n", b));
			}
		}
	}

	KdPrint(("GetRawResult: read %lu bytes  [%02X %02X %02X  %02X %02X %02X %02X]\n", u, pb_[0], pb_[1], pb_[2], pb_[3], pb_[4], pb_[5], pb_[6]));

	return STATUS_SUCCESS;
}


void SetRawDataRate (PEXTRA_DEVICE_EXTENSION /*edx*/, UCHAR rate_)
{
	WRITE_PORT_UCHAR(FDC_DSR_PORT, rate_);
}

void MotorOn (PEXTRA_DEVICE_EXTENSION edx, BOOLEAN fInterrupts_)
{
	UCHAR bFlags = DRVCTL_ENABLE_CONTROLLER | (fInterrupts_ ? DRVCTL_ENABLE_DMA_AND_INTERRUPTS : 0);
	WRITE_PORT_UCHAR(FDC_DOR_PORT, bFlags | (FDC_MOTOR_A << edx->DeviceUnit) | (FDC_SELECT_A + edx->DeviceUnit));
}

void MotorOff (PEXTRA_DEVICE_EXTENSION edx, BOOLEAN fInterrupts_)
{
	UCHAR bFlags = DRVCTL_ENABLE_CONTROLLER | (fInterrupts_ ? DRVCTL_ENABLE_DMA_AND_INTERRUPTS : 0);
	WRITE_PORT_UCHAR(FDC_DOR_PORT, bFlags | 								   (FDC_SELECT_A + edx->DeviceUnit));
}

void EnableUnit (PEXTRA_DEVICE_EXTENSION edx, BOOLEAN fInterrupts_)
{
	UCHAR bFlags = DRVCTL_ENABLE_CONTROLLER | (fInterrupts_ ? DRVCTL_ENABLE_DMA_AND_INTERRUPTS : 0);
	WRITE_PORT_UCHAR(FDC_DOR_PORT, bFlags | (FDC_MOTOR_A << edx->DeviceUnit) | (FDC_SELECT_A + edx->DeviceUnit));
}

void DisableUnit (PEXTRA_DEVICE_EXTENSION edx, BOOLEAN fInterrupts_)
{
	UCHAR bFlags =							  (fInterrupts_ ? DRVCTL_ENABLE_DMA_AND_INTERRUPTS : 0);
	WRITE_PORT_UCHAR(FDC_DOR_PORT, bFlags | (FDC_MOTOR_A << edx->DeviceUnit) | (FDC_SELECT_A + edx->DeviceUnit));
}

void SwitchUnit (PEXTRA_DEVICE_EXTENSION edx)
{
	UCHAR bFlags = DRVCTL_ENABLE_CONTROLLER | DRVCTL_ENABLE_DMA_AND_INTERRUPTS | (edx->DriveOnValue ^ 1);
	WRITE_PORT_UCHAR(FDC_DOR_PORT, bFlags);
	KeStallExecutionProcessor(10);
}

void RestoreUnit (PEXTRA_DEVICE_EXTENSION edx)
{
	UCHAR bFlags = DRVCTL_ENABLE_CONTROLLER | DRVCTL_ENABLE_DMA_AND_INTERRUPTS | edx->DriveOnValue;
	WRITE_PORT_UCHAR(FDC_DOR_PORT, bFlags);
	KeStallExecutionProcessor(10);
}

///////////////////////////////////////////////////////////////////////////////

NTSTATUS FormatTrack (PEXTRA_DEVICE_EXTENSION edx, PIRP Irp)
{
	PFD_FORMAT_PARAMS pp = (PFD_FORMAT_PARAMS)Irp->AssociatedIrp.SystemBuffer;
	ULONG Size = sizeof(FD_ID_HEADER) * pp->sectors;

	if (Size > edx->IoBufferSize)
		return STATUS_INSUFFICIENT_RESOURCES;

	RtlCopyMemory(edx->IoBuffer, pp->Header, Size);

#if DBG
	KdPrint(("Formatting %u sectors, size=%u, gap=%u, fill=%u\n", pp->sectors, pp->size, pp->gap, pp->fill));
	for (ULONG u = 0 ; u < Size ; u += sizeof(FD_ID_HEADER))
	{
		PUCHAR p = edx->IoBuffer + u;
		KdPrint(("  %02X %02X %02X %02X\n", p[0], p[1], p[2], p[3]));
	}
#endif

	NTSTATUS status = CommandFormatTrack(edx, edx->IoBufferMdl, Size, pp->flags, pp->phead, pp->size, pp->sectors, pp->gap, pp->fill);

	if (NT_SUCCESS(status))
		status = CheckFdcResult(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandFormatTrack failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));

	return status;
}

NTSTATUS FormatAndWrite (PEXTRA_DEVICE_EXTENSION edx, PIRP Irp, ULONG DataSize)
{
	PFD_FORMAT_PARAMS pp = (PFD_FORMAT_PARAMS)Irp->AssociatedIrp.SystemBuffer;
	ULONG Size = DataSize - sizeof(FD_FORMAT_PARAMS);

	if (Size > edx->IoBufferSize)
		return STATUS_INSUFFICIENT_RESOURCES;

	RtlCopyMemory(edx->IoBuffer, pp->Header, Size);

#if DBG
	KdPrint(("Formatting %u sectors, size=%u, gap=%u\n", pp->sectors, pp->size, pp->gap));
#endif

	NTSTATUS status = CommandFormatAndWrite(edx, edx->IoBufferMdl, Size, pp->flags, pp->phead, pp->size, pp->sectors, pp->gap);

	if (NT_SUCCESS(status))
		status = CheckFdcResult(edx);

	if (!NT_SUCCESS(status))
		KdPrint(("!!! CommandFormatAndWrite failed with %X [ST0:%02X ST1:%02X ST2:%02X]\n", status, edx->FifoOut[0], edx->FifoOut[1], edx->FifoOut[2]));

	return status;
}

NTSTATUS WaitIndex (PEXTRA_DEVICE_EXTENSION edx, bool CalcSpinTime=false)
{
	// Using READ_TRACK can also give the start of track position, by over-reading until End of Cylinder is returned.
	// It gives a tighter index sync than reading an error sector, but relies on the command not failing earlier due
	// to CRC errors or index header details not matching the original request.  Spectaculator uses this method.
	//
	// Reading a non-existent sector is a better method overall as it doesn't rely on any aspect of the track data.
	// We can miss the very first sector if it's too close to the start, but we can detect this by comparing the
	// wrapped time with the time offset for the first sector.

	static UCHAR bSector = 0xef;
	NTSTATUS status = STATUS_SUCCESS;

	KdPrint(("WaitIndex()\n"));

	for ( ; NT_SUCCESS(status) ; bSector -= 19)
	{
		status = CommandReadWrite(edx, edx->IoBufferMdl, SectorSize(1), COMMND_READ_DATA, COMMND_OPTION_MFM, 0, bSector, bSector^0x55, bSector^0xaa, 1, bSector, 1, 0xff);

		// Stop once we find a non-existent sector, or if the track is blank
		if (status == STATUS_NONEXISTENT_SECTOR || status == STATUS_FLOPPY_ID_MARK_NOT_FOUND)
		{
			status = STATUS_SUCCESS;
			break;
		}

		// Ignore CRC errors in the dummy sector, in the extremely unlikely event it exists
		if (status == STATUS_CRC_ERROR)
			status = STATUS_SUCCESS;
	}

	if (NT_SUCCESS(status) && CalcSpinTime)
	{
		LARGE_INTEGER iFreq, iTime0, iTime1;
		iTime0 = KeQueryPerformanceCounter(NULL);
		status = CommandReadWrite(edx, edx->IoBufferMdl, SectorSize(1), COMMND_READ_DATA, COMMND_OPTION_MFM, 0, bSector, bSector^0x55, bSector^0xaa, 1, bSector, 1, 0xff);
		iTime1 = KeQueryPerformanceCounter(&iFreq);

		// Ensure the second read failed with an expected error, before calculating the speed
		// This avoids a bogus time when the second read timed out, or failed with another error
		if (status == STATUS_NONEXISTENT_SECTOR || status == STATUS_FLOPPY_ID_MARK_NOT_FOUND)
		{
			status = STATUS_SUCCESS;
			edx->SpinTime = (ULONG)((iTime1.QuadPart - iTime0.QuadPart) * 500000i64 / iFreq.QuadPart);	// halved to give 1 revolution
			KdPrint(("### Disk spin time = %luus\n", edx->SpinTime));
  		}
	}

	return status;
}

NTSTATUS WaitSector (PEXTRA_DEVICE_EXTENSION edx, UCHAR flags, UCHAR head, UCHAR sectors)
{
	NTSTATUS status = WaitIndex(edx);
	UCHAR Errors = 0;

	for (UCHAR i = 0 ; NT_SUCCESS(status) && i < sectors ; i++)
	{
		if (NT_SUCCESS(status = CommandReadId(edx, flags, head)))
			Errors = 0;
		else if (status != STATUS_CRC_ERROR && status != STATUS_NONEXISTENT_SECTOR)
			break;
		else if (++Errors >= 3) // fail after 3 consecutive errors
			break;
		else
		{
			KdPrint(("!!! ReadId error #%u (%X)\n", Errors, status));
			status = STATUS_SUCCESS;

			// Don't count the error header
			i--;
		}
	}

	edx->WaitSector = FALSE;

	return status;
}

NTSTATUS CheckMedia (PEXTRA_DEVICE_EXTENSION edx)
{
	UCHAR DriveStatus = 0;
	NTSTATUS status = STATUS_SUCCESS;

	if (NT_SUCCESS(status = GetFdcDiskChange(edx, &DriveStatus)) && (DriveStatus & DSKCHG_DISKETTE_REMOVED))
	{
		// Recalibrate if required, to ensure the head is at a known position
		if (edx->NeedRecalibrate && NT_SUCCESS(CommandRecalibrate(edx)))
			edx->NeedRecalibrate = FALSE;

		// Determine which way is safe to step
		int nOffset = edx->PhysCyl ? -1 : +1;

		KdPrint(("Disk change set, seeking to %u then %u\n", edx->PhysCyl+nOffset, edx->PhysCyl));
		CommandSeek(edx, 0, (UCHAR)(edx->PhysCyl+nOffset));
		CommandSeek(edx, 0, (UCHAR)(edx->PhysCyl-nOffset));

		if (NT_SUCCESS(status = GetFdcDiskChange(edx, &DriveStatus)) && (DriveStatus & DSKCHG_DISKETTE_REMOVED))
		{
			KdPrint(("!!! No disk in drive!\n"));
			status = STATUS_NO_MEDIA;
		}
	}

	return status;
}


NTSTATUS TimedScanTrack (PEXTRA_DEVICE_EXTENSION edx, PIRP Irp)
{
	PFD_SCAN_PARAMS pp = (PFD_SCAN_PARAMS)Irp->AssociatedIrp.SystemBuffer;
	PFD_TIMED_SCAN_RESULT po = (PFD_TIMED_SCAN_RESULT)edx->IoBuffer;

	NTSTATUS status = WaitIndex(edx, !edx->SpinTime);

	LARGE_INTEGER iStart = KeQueryPerformanceCounter(NULL);
	ULONG ulFirstTime = 0;

	UCHAR Count = 0, Errors = 0;
	while (NT_SUCCESS(status))
	{
		// Read a single header
		if (!NT_SUCCESS(status = CommandReadId(edx, pp->flags, pp->head)))
		{
			// Blank tracks are valid
			if (status == STATUS_FLOPPY_ID_MARK_NOT_FOUND)
				status = STATUS_SUCCESS;

			// Tolerate some ID header errors
			else if (status == STATUS_CRC_ERROR || status == STATUS_NONEXISTENT_SECTOR)
			{
				KdPrint(("!!! ReadId error #%u (%X)\n", Errors, status));
				status = STATUS_SUCCESS;

				// Retry unless there have been 3 consecutive errors
				if (++Errors < 3)
					continue;

				KdPrint(("!!! Too many header errors, assuming blank\n"));
			}

			// Finish the scan, with or without error
			break;
		}

		LARGE_INTEGER iFreq = {0}, iNow = KeQueryPerformanceCounter(&iFreq);
		ULONG ulDiff = (ULONG)((iNow.QuadPart - iStart.QuadPart) * 1000000i64 / iFreq.QuadPart);
		if (!ulFirstTime) ulFirstTime = ulDiff;

		if (ulDiff > edx->SpinTime)
		{
			KdPrint(("### Past end of track time by %luus\n", ulDiff-edx->SpinTime));

			// Are we within 2% of the looped first sector we detected?
			if ((ulDiff - edx->SpinTime) + (edx->SpinTime * 2/100) > ulFirstTime)
			{
				UCHAR u;

				// Find the sector in the list we previously scanned, as sectors close to the index
				// hole aren't always visible on the 2nd pass!!
				for (u = 0 ; po->Header[u].sector != edx->FifoOut[5] && u < Count ; u++) ;
				if (u == Count) u = 0;	// Safe fall back on 1st sector

				edx->SpinTime = ulDiff-po->Header[u].reltime;
				KdPrint(("### Track loop found @%u, new SpinTime = %luus\n", u, edx->SpinTime));
				break;
			}
		}

		po->Header[Count].cyl = edx->FifoOut[3];
		po->Header[Count].head = edx->FifoOut[4];
		po->Header[Count].sector = edx->FifoOut[5];
		po->Header[Count].size = edx->FifoOut[6];
		po->Header[Count].reltime = ulDiff;
		KdPrint(("reltime[%u] = %lu\n", Count, po->Header[Count].reltime));

		if (++Count == 0xff)
			status = STATUS_BUFFER_TOO_SMALL;

		// Clear error count for the next header
		Errors = 0;
	}

	po->count = Count;
	po->firstseen = 0;
	po->tracktime = edx->SpinTime;
	KdPrint(("Count = %u, TrackTime = %luus\n", po->count, po->tracktime));

	// Normalise the offsets of wrapped sectors
	for (UCHAR k = 0 ; k < Count ; k++)
		if (po->Header[k].reltime >= edx->SpinTime)
			po->Header[k].reltime -= edx->SpinTime;

	for (UCHAR i = 0, b ; i < Count ; i++)
	{
		for (UCHAR j = b = i ; j < Count ; j++)
			if (po->Header[j].reltime < po->Header[b].reltime)
				b = j;

		FD_TIMED_ID_HEADER h = po->Header[i];
		po->Header[i] = po->Header[b];
		po->Header[b] = h;

		// Remember the offset of the first sector seen
		if (!i) po->firstseen = b;
	}

	return status;
}

///////////////////////////////////////////////////////////////////////////////

VOID ShortCommandThreadProc (PVOID StartContext)
{
	PEXTRA_DEVICE_EXTENSION edx = (PEXTRA_DEVICE_EXTENSION)StartContext;
	KdPrint(("ShortCommandThreadProc with threshold=%lu and delay=%luus\n", edx->DmaThreshold, edx->DmaDelay));

	if (!edx->ppAdapterObject)
	{
		KdPrint(("!!! ppAdapterObject is NULL in ShortCommandThreadProc!\n"));
		KeSetEvent(&edx->dmasync, 0, FALSE);
	}
	else
	{
		PDMA_ADAPTER pAdapter = *edx->ppAdapterObject;
		PREAD_DMA_COUNTER pRDC = pAdapter->DmaOperations->ReadDmaCounter;

		// Disable the FIFO, so the DMA count reflects exactly how much data is left
		CommandConfigure(edx, 0x30, edx->PreTrk);

		// Note the starting DMA value, before we release the calling thread
		ULONG ulStart = pRDC(pAdapter), ulCount = 0;

		// Signal to the caller that we're ready for the command to start
		KdPrint(("### ShortCommandThreadProc is ready to go!\n"));
		KeSetEvent(&edx->dmasync, 0, FALSE);

		// Loop until the value changes, indicating the real transfer length has been set
		KdPrint(("### Waiting for Count to change (currently %lu)\n", pRDC(pAdapter)));
		while (!edx->StopDmaThread && pRDC(pAdapter) == ulStart);

		// Loop until we reach the required DMA value
		KdPrint(("### Waiting for Count to reach threshold (%lu)\n", edx->DmaThreshold));
		while (!edx->StopDmaThread && (ulCount = pRDC(pAdapter)) > edx->DmaThreshold);

		// Only interrupt the controller if we weren't cancelled
		if (!edx->StopDmaThread)
		{
			KeStallExecutionProcessor(edx->DmaDelay);
			DisableUnit(edx, FALSE);

			KdPrint(("### Reached DMA target (now %lu)\n", ulCount));
			KeStallExecutionProcessor(1000);

			EnableUnit(edx, TRUE);

			GetRawResult(edx->FifoOut, 7);

			edx->FifoIn[0] = (edx->FifoIn[0]&COMMND_OPTION_MFM) | 0x0A; // COMMND_READ_ID
			edx->FifoIn[1] = (0 << 2) | edx->DeviceUnit;
			SendRawCommand(edx->FifoIn, 2);

			edx->NeedReset = TRUE;

			KdPrint(("### DMA seen at Count=%lu (now %lu)\n", ulCount, pRDC(pAdapter)));
		}
		else
		{
			KdPrint(("!!! StopDmaThread was set before reaching target!\n"));
		}
	}

	KdPrint(("Leaving ShortCommandThreadProc()\n"));
	PsTerminateSystemThread(STATUS_SUCCESS);
}

VOID DorSwitchThreadProc (PVOID StartContext)
{
	PEXTRA_DEVICE_EXTENSION edx = (PEXTRA_DEVICE_EXTENSION)StartContext;
	KdPrint(("DorSwitchThreadProc with threshold=%lu\n", edx->DmaThreshold));

	if (!edx->ppAdapterObject)
	{
		KdPrint(("!!! ppAdapterObject is NULL in DorSwitchThreadProc!\n"));
		KeSetEvent(&edx->dmasync, 0, FALSE);
	}
	else
	{
		PDMA_ADAPTER pAdapter = *edx->ppAdapterObject;
		PREAD_DMA_COUNTER pRDC = pAdapter->DmaOperations->ReadDmaCounter;

		// Note the starting DMA value, before we release the calling thread
		ULONG ulStart = pRDC(pAdapter), ulCount = 0;

		// Signal to the caller that we're ready for the command to start
		KdPrint(("### DorSwitchThreadProc is ready to go!\n"));
		KeSetEvent(&edx->dmasync, 0, FALSE);

		// Loop until the value changes, indicating the real transfer length has been set
		KdPrint(("### Waiting for Count to change (currently %lu)\n", pRDC(pAdapter)));
		while (!edx->StopDmaThread && pRDC(pAdapter) == ulStart);

		// Loop until we reach the required DMA value
		KdPrint(("### Waiting for Count to reach threshold (%lu)\n", edx->DmaThreshold));
		while (!edx->StopDmaThread && (ulCount = pRDC(pAdapter)) > edx->DmaThreshold);

		// Only switch the drive select if we weren't cancelled
		if (!edx->StopDmaThread)
		{
			// Switch back to the main unit
			SwitchUnit(edx);

			// Also set the new data rate if it's different from the starting 500Kbps
			if (edx->DataRate != 0)
				SetRawDataRate(edx, edx->DataRate);

			KdPrint(("### Switched DOR to other device (was %lu, now %lu)\n", ulCount, pRDC(pAdapter)));
		}
		else
		{
			KdPrint(("!!! StopDmaThread was set before reaching target!\n"));
		}
	}

	KdPrint(("Leaving DorSwitchThreadProc()\n"));
	PsTerminateSystemThread(STATUS_SUCCESS);
}


VOID ThreadProc (PVOID StartContext)
{
	PEXTRA_DEVICE_EXTENSION edx = (PEXTRA_DEVICE_EXTENSION)StartContext;
	KdPrint((DRIVERNAME " - Entering Thread ThreadProc()\n"));

	NTSTATUS status;

	KeSetPriorityThread(KeGetCurrentThread(), LOW_REALTIME_PRIORITY);

	edx->Acquired = edx->Enabled = FALSE;
	SetDefaults(edx);

	for (;;)
	{
		LARGE_INTEGER MotorTimeout, AcquireTimeout;
		MotorTimeout.QuadPart = edx->MotorTimeout ? SECONDS(edx->MotorTimeout) : SECONDS(1);	// use 1 second polls for no motor timeout
		AcquireTimeout.QuadPart = (LONGLONG)-1;

		// If we've acquired or enabled the device, set an appropriate timeout to release it
		status = KeWaitForSingleObject(&edx->semaphore, Executive, KernelMode, FALSE,
										edx->Enabled ? &MotorTimeout : edx->Acquired ? &AcquireTimeout : NULL);

		// Force a handle status if required, as needed when we're being unloaded
		if (!NT_SUCCESS(edx->Status))
			break;

		// If the motor timeout has expired, switch it off
		if (status == STATUS_TIMEOUT)
		{
			// Only consider switching the motor off if a timeout is set
			if (edx->MotorTimeout)
			{
				if (edx->Enabled)
					DisableFdc(edx);

				if (edx->Acquired)
				{
					ReleaseFdc(edx);
					edx->Acquired = FALSE;
				}
			}

			// Go back to waiting for a request/timeout
			continue;
		}

		// Ignore any other wait failure (should never happen)
		if (!NT_SUCCESS(status))
			continue;

		// Check the next IRP exists, as it may have been cancelled and removed from the queue already
		PIRP Irp = IoCsqRemoveNextIrp(&edx->IrpQueue, 0);
		if (!Irp)
			continue;

		PIO_STACK_LOCATION stack = IoGetCurrentIrpStackLocation(Irp);


		// Allocate the buffer if not already allocated
		if (!edx->IoBuffer)
		{
			FlAllocateIoBuffer(edx, 0x8000);	// 32K

			// Fail the request if we couldn't
			if (!edx->IoBuffer)
			{
				CompleteRequest(Irp, STATUS_INSUFFICIENT_RESOURCES);
				continue;
			}
		}

		// Acquire the controller if necessary
		if (edx->Acquired)
			status = STATUS_SUCCESS;
		else if (NT_SUCCESS(status = AcquireFdc(edx, 4)))
			edx->Acquired = TRUE;
		else
		{
			CompleteRequest(Irp, status);
			continue;
		}


		// Pick up the IOCTL code if the request is from DeviceIoControl, otherwise use zero
		// This allows us to handle other major functions without too much nested code
		ULONG IoCode = (stack->MajorFunction == IRP_MJ_DEVICE_CONTROL) ? stack->Parameters.DeviceIoControl.IoControlCode : 0;

		// The following requests can be processed without starting the motor
		switch (IoCode)
		{
			case IOCTL_FDRAWCMD_GET_VERSION:
			{
				status = CheckBuffers(Irp, 0, sizeof(UINT32));

				if (NT_SUCCESS(status))
					*(PUINT32)Irp->AssociatedIrp.SystemBuffer = DRIVER_VERSION;

				CompleteRequest(Irp, status, sizeof(UINT32));
				continue;
			}

			case IOCTL_FD_GET_RESULT:
			{
				status = CheckBuffers(Irp, 0, sizeof(FD_CMD_RESULT));

				if (NT_SUCCESS(status))
					CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_SENSE_INT_STATUS:
			{
				status = CheckBuffers(Irp, 0, sizeof(FD_INTERRUPT_STATUS));

				if (NT_SUCCESS(status))
					CopyOutput(edx, Irp, sizeof(FD_INTERRUPT_STATUS));

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FD_SET_MOTOR_TIMEOUT:
			{
				PUCHAR pbTimeout = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(UCHAR), 0);

				if (NT_SUCCESS(status))
				{
					if (*pbTimeout <= 3)
						edx->MotorTimeout = *pbTimeout;
					else
						status = STATUS_INVALID_PARAMETER;
				}

				CompleteRequest(Irp, status);
				continue;
			}

			case IOCTL_FD_SET_HEAD_SETTLE_TIME:
			{
				PUCHAR pbHeadSettleTime = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(UCHAR), 0);

				if (NT_SUCCESS(status))
				{
					if (*pbHeadSettleTime)
						edx->HeadSettleTime = *pbHeadSettleTime;
					else
						status = STATUS_INVALID_PARAMETER;
				}

				CompleteRequest(Irp, status);
				continue;
			}

			case IOCTL_FD_SET_DISK_CHECK:
			{
				KdPrint(("In IOCTL_FD_SET_DISK_CHECK\n"));
				PUCHAR pbNeedDisk = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(UCHAR), sizeof(UCHAR), true);

				if (NT_SUCCESS(status))
				{
					UCHAR bOldNeedDisk = edx->NeedDisk;
					edx->NeedDisk = !!*pbNeedDisk;
					*pbNeedDisk = bOldNeedDisk;
					KdPrint(("edx->NeedDisk is now %lu\n", edx->NeedDisk));
				}

				CompleteRequest(Irp, status, sizeof(UCHAR));
				continue;
			}

			case IOCTL_FD_SET_SECTOR_OFFSET:
			{
				PFD_SECTOR_OFFSET_PARAMS pp = (PFD_SECTOR_OFFSET_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_SECTOR_OFFSET_PARAMS), 0);

				if (NT_SUCCESS(status))
				{
					KdPrint(("IOCTL_FD_SET_SECTOR_OFFSET: sector offset = %u\n", pp->sectors));
					edx->WaitSector = TRUE;
					edx->WaitSectorCount = pp->sectors;
				}

				CompleteRequest(Irp, status);
				continue;
			}

			case IOCTL_FD_SET_SHORT_WRITE:
			{
				PFD_SHORT_WRITE_PARAMS pp = (PFD_SHORT_WRITE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_SHORT_WRITE_PARAMS), 0);

				if (NT_SUCCESS(status))
				{
					KdPrint(("IOCTL_FD_SET_SHORT_WRITE: length=%lu, finetune=%lu\n", pp->length, pp->finetune));
					edx->ShortWrite = !(pp->finetune & ~0xffff);	// delay must be < 65536
					edx->DmaThreshold = pp->length;
					edx->DmaDelay = pp->finetune;
				}

				CompleteRequest(Irp, status);
				continue;
			}

			// Obsolete
			case IOCTL_FD_LOCK_FDC:
			case IOCTL_FD_UNLOCK_FDC:
				CompleteRequest(Irp, STATUS_SUCCESS, 0);
				continue;

			case IOCTL_FD_MOTOR_OFF:
			{
				status = edx->Enabled ? DisableFdc(edx) : STATUS_SUCCESS;
				CompleteRequest(Irp, status);
				continue;
			}
		}

		// The following requests can be processed without a disk in the drive
		switch (IoCode)
		{
			case IOCTL_FDCMD_VERSION:
			{
				status = CheckBuffers(Irp, 0, sizeof(UCHAR));

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandVersion(edx)))
					CopyOutput(edx, Irp, sizeof(UCHAR));

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_PART_ID:
			{
				status = CheckBuffers(Irp, 0, sizeof(UCHAR));

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandPartId(edx)))
					CopyOutput(edx, Irp, sizeof(UCHAR));

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_DUMPREG:
			{
				status = CheckBuffers(Irp, 0, sizeof(FD_DUMPREG_RESULT));

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandDumpReg(edx)))
					CopyOutput(edx, Irp, sizeof(FD_DUMPREG_RESULT));

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_CONFIGURE:
			{
				PFD_CONFIGURE_PARAMS pp = (PFD_CONFIGURE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_CONFIGURE_PARAMS), sizeof(FD_CMD_RESULT), true);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandConfigure(edx, pp->eis_efifo_poll_fifothr, pp->pretrk)))
				{
					edx->EisEfifoPollFifothr = pp->eis_efifo_poll_fifothr;
					edx->PreTrk = pp->pretrk;
					CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));
				}

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_SPECIFY:
			{
				PFD_SPECIFY_PARAMS pp = (PFD_SPECIFY_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_SPECIFY_PARAMS), sizeof(FD_CMD_RESULT), true);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandSpecify(edx, pp->srt_hut, pp->hlt_nd)))
				{
					edx->StepRateHeadUnloadTime = pp->srt_hut;
					edx->HeadLoadTimeNoDMA = pp->hlt_nd;
					CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));
				}

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_PERPENDICULAR_MODE:
			{
				PFD_PERPENDICULAR_PARAMS pp = (PFD_PERPENDICULAR_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_PERPENDICULAR_PARAMS), sizeof(FD_CMD_RESULT), true);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandPerpendicularMode(edx, pp->ow_ds_gap_wgate)))
				{
					edx->PerpendicularMode = pp->ow_ds_gap_wgate;
					CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));
				}

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FD_SET_DATA_RATE:
			{
				KdPrint(("### IOCTL_FD_SET_DATA_RATE\n"));
				PUCHAR pbRate = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(UCHAR), 0);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = SetFdcDataRate(edx, *pbRate)))
					edx->DataRate = *pbRate;

				KdPrint(("<- IOCTL_FD_SET_DATA_RATE\n"));
				CompleteRequest(Irp, status);
				continue;
			}

			case IOCTL_FD_RESET:
			{
				KdPrint(("### IOCTL_FD_RESET\n"));
				status = ResetFdc(edx);
				edx->NeedReset = FALSE;

				if (NT_SUCCESS(status))
					edx->NeedInit = edx->NeedRecalibrate = TRUE;
				else
					edx->NeedReset = TRUE;

				CompleteRequest(Irp, status);
				continue;
			}

			case IOCTL_FD_GET_REMAIN_COUNT:
			{
				PUINT32 pulRemain = (PUINT32)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, 0, sizeof(UINT32));

				if (NT_SUCCESS(status))
				{
					if (!edx->ppAdapterObject)
						edx->ppAdapterObject = FindAdapterObject(edx->pdx->Pdo);

					if (!edx->ppAdapterObject)
						status = STATUS_UNSUCCESSFUL;
					else
					{
						PDMA_ADAPTER pAdapter = *edx->ppAdapterObject;
						PREAD_DMA_COUNTER pRDC = pAdapter->DmaOperations->ReadDmaCounter;
						*pulRemain = pRDC(pAdapter);

						KdPrint(("### IOCTL_FD_GET_REMAIN_COUNT: DMA counter is %lu\n", *pulRemain));
					}
				}

				CompleteRequest(Irp, status, sizeof(UINT32));
				continue;
			}

			case IOCTL_FD_GET_FDC_INFO:
			{
				FDC_INFO fdci;
				RtlZeroMemory(&fdci, sizeof(fdci));

				status = CheckBuffers(Irp, 0, sizeof(FD_FDC_INFO));

				if (NT_SUCCESS(status) && NT_SUCCESS(status = GetFdcInfo(edx, &fdci)))
				{
					PFD_FDC_INFO p = (PFD_FDC_INFO)Irp->AssociatedIrp.SystemBuffer;
					p->ControllerType = fdci.FloppyControllerType;
					p->SpeedsAvailable = fdci.SpeedsAvailable;
					p->BusType = (UCHAR)fdci.BusType;
					p->BusNumber = fdci.BusNumber;
					p->ControllerNumber = fdci.ControllerNumber;
					p->PeripheralNumber = fdci.PeripheralNumber;
				}

				CompleteRequest(Irp, status, sizeof(FD_FDC_INFO));
				continue;
			}

			case IOCTL_FD_MOTOR_ON:
			{
				PUCHAR pp = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;

				// Nothing to do if both motors weren't requested
				if (!stack->Parameters.DeviceIoControl.InputBufferLength || !*pp)
					break;

				// Fall through...
			}

			case IOCTL_FD_RAW_READ_TRACK:
			{
				// Both motors not already enabled?
				if ((edx->DriveOnValue & (FDC_MOTOR_A|FDC_MOTOR_B)) != (FDC_MOTOR_A|FDC_MOTOR_B))
				{
					KdPrint(("Pairing motors...\n"));

					// Select both now
					edx->DriveOnValue |= (FDC_MOTOR_A|FDC_MOTOR_B);

					// If the first motor is already running, start the 2nd motor and wait for it to settle
					if (edx->Enabled)
					{
						RestoreUnit(edx);
						Sleep(edx->MotorSettleTimeWrite);
					}
				}

				// Continue to 2nd-stage processing below...
				break;
			}
		}

		if (edx->NeedReset && NT_SUCCESS(status = ResetFdc(edx)))
		{
			KdPrint(("### NeedReset\n"));
			edx->NeedReset = FALSE;
			edx->NeedInit = edx->NeedRecalibrate = TRUE;
		}

		if (NT_SUCCESS(status) && !edx->Enabled && NT_SUCCESS(status = EnableFdc(edx, edx->MotorSettleTimeWrite)))
		{
			edx->NeedInit = TRUE;
			edx->SpinTime = 0;
		}

		if (NT_SUCCESS(status) && edx->NeedInit)
		{
			KdPrint(("### NeedInit\n"));

			if (edx->PerpendicularMode)
				CommandPerpendicularMode(edx, edx->PerpendicularMode);

			CommandConfigure(edx, edx->EisEfifoPollFifothr, edx->PreTrk);
			CommandSpecify(edx, edx->StepRateHeadUnloadTime, edx->HeadLoadTimeNoDMA);
			SetFdcDataRate(edx, edx->DataRate);

			edx->NeedInit = FALSE;
		}

		if (!NT_SUCCESS(status))
		{
			KdPrint(("Failing request with %X\n", status));
			CompleteRequest(Irp, status);
			continue;
		}

		// The following requests the controller be initialised, but don't require a disk in the drive
		switch (IoCode)
		{
			case IOCTL_FDCMD_RECALIBRATE:
			{
				status = CheckBuffers(Irp, 0, sizeof(FD_INTERRUPT_STATUS), true);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandRecalibrate(edx)))
				{
					CopyOutput(edx, Irp, sizeof(FD_INTERRUPT_STATUS));
					edx->NeedRecalibrate = FALSE;
				}

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_SEEK:
			{
				PUCHAR pp = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(UCHAR), sizeof(FD_INTERRUPT_STATUS), true);

				// Recalibrate before the absolute seek if the controller track counter could be out of sync
				if (NT_SUCCESS(status) && edx->NeedRecalibrate && NT_SUCCESS(status = CommandRecalibrate(edx)))
					edx->NeedRecalibrate = FALSE;

				if (NT_SUCCESS(status))
				{
					// Use any supplied head value, or zero otherwise
					UCHAR cyl = *pp;
					UCHAR head = (stack->Parameters.DeviceIoControl.InputBufferLength >= 2) ? pp[1] : 0;

					if (cyl >= MAX_CYLS)
						status = STATUS_INVALID_PARAMETER;
					else if (NT_SUCCESS(status = CommandSeek(edx, head, cyl)))
						CopyOutput(edx, Irp, sizeof(FD_INTERRUPT_STATUS));
				}

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}

			case IOCTL_FDCMD_RELATIVE_SEEK:
			{
				PFD_RELATIVE_SEEK_PARAMS pp = (PFD_RELATIVE_SEEK_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_RELATIVE_SEEK_PARAMS), sizeof(FD_INTERRUPT_STATUS), true);

				if (NT_SUCCESS(status))
				{
					ULONG newcyl = (pp->flags & COMMND_OPTION_DIRECTION) ? (edx->PhysCyl+pp->offset) : (edx->PhysCyl-pp->offset);

					if (newcyl >= MAX_CYLS)
						status = STATUS_INVALID_PARAMETER;
					else if (NT_SUCCESS(status = CommandRelativeSeek(edx, pp->flags, pp->head, pp->offset)))
						CopyOutput(edx, Irp, sizeof(FD_INTERRUPT_STATUS));
				}

				CompleteRequest(Irp, status, Irp->IoStatus.Information);
				continue;
			}
		}

		// Check for a disk in the drive, if required or requested
		if (NT_SUCCESS(status) && (edx->NeedDisk || IoCode == IOCTL_FD_CHECK_DISK))
			status = CheckMedia(edx);

		if (!NT_SUCCESS(status))
		{
			KdPrint(("Failing request with %X\n", status));
			CompleteRequest(Irp, status);
			continue;
		}

		// The following requests require a disk in the drive, optionally checked above
		switch (IoCode)
		{
			case IOCTL_FDCMD_READ_ID:
			{
				PFD_READ_ID_PARAMS pp = (PFD_READ_ID_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_READ_ID_PARAMS), sizeof(FD_CMD_RESULT));

				if (NT_SUCCESS(status) && edx->WaitSector)
					status = WaitSector(edx, pp->flags, pp->head, edx->WaitSectorCount);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandReadId(edx, pp->flags, pp->head)))
					CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));

				break;
			}

			case IOCTL_FDCMD_READ_TRACK:
			{
				PFD_READ_WRITE_PARAMS pp = (PFD_READ_WRITE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				ULONG DataSize = stack->Parameters.DeviceIoControl.OutputBufferLength;
				status = CheckBuffers(Irp, sizeof(FD_READ_WRITE_PARAMS), 1);

				if (NT_SUCCESS(status))
					status = CommandReadTrack(edx, Irp->MdlAddress, DataSize, pp->flags, pp->phead, pp->cyl, pp->head, pp->sector, pp->size, pp->eot, pp->gap, pp->datalen);

				if (NT_SUCCESS(status))
				{
					ULONG ulRead = (edx->FifoOut[5] - pp->sector) * SectorSize(pp->size); // safe
					Irp->IoStatus.Information = (ulRead <= DataSize) ? ulRead : 0;
					KdPrint(("### Successfully read %u-%u=%u sectors (%lu bytes)\n", edx->FifoOut[5], pp->sector, edx->FifoOut[5] - pp->sector, Irp->IoStatus.Information));
				}

				break;
			}

			case IOCTL_FDCMD_READ_DATA:
			case IOCTL_FDCMD_READ_DELETED_DATA:
			{
				KdPrint(("In IOCTL_FDCMD_READ_[DELETED_]DATA\n"));
				PFD_READ_WRITE_PARAMS pp = (PFD_READ_WRITE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				ULONG DataSize = 0;

				status = CheckBuffers(Irp, sizeof(FD_READ_WRITE_PARAMS), 1);

				if (NT_SUCCESS(status))
				{
					UCHAR bSectors = pp->eot - pp->sector;

					if (!bSectors)
						status = STATUS_INVALID_PARAMETER;
					else if (pp->size > 7)
					{
						DataSize = stack->Parameters.DeviceIoControl.OutputBufferLength;
						KdPrint(("### Using supplied data buffer size of %lu bytes\n", DataSize));
					}
					else
					{
						DataSize = bSectors * SectorSize(pp->size); // safe
						status = CheckBuffers(Irp, 0, DataSize);
						KdPrint(("### Expected data size = %lu bytes (status=%08lx)\n", DataSize, status));
					}
				}

				if (NT_SUCCESS(status) && edx->WaitSector)
					status = WaitSector(edx, pp->flags, pp->phead, edx->WaitSectorCount);

				if (NT_SUCCESS(status))
				{
					UCHAR bCommand = (IoCode == IOCTL_FDCMD_READ_DATA) ? COMMND_READ_DATA : COMMND_READ_DELETED_DATA;
					status = CommandReadWrite(edx, Irp->MdlAddress, DataSize, bCommand, pp->flags, pp->phead, pp->cyl, pp->head, pp->sector, pp->size, pp->eot, pp->gap, pp->datalen);
				}
#if DBG
				if (pp->size > 7 && (edx->ppAdapterObject || (edx->ppAdapterObject = FindAdapterObject(edx->pdx->Pdo))))
				{
					PDMA_ADAPTER pAdapter = *edx->ppAdapterObject;
					PREAD_DMA_COUNTER pRDC = pAdapter->DmaOperations->ReadDmaCounter;
					KdPrint(("### After long read, DMA counter is %lu\n", pRDC(pAdapter)));
				}
#endif
				if (NT_SUCCESS(status))
				{
					UCHAR bSectors = edx->FifoOut[5] - pp->sector;
					ULONG ulRead = bSectors * SectorSize(pp->size); // safe
					Irp->IoStatus.Information = (ulRead <= DataSize) ? ulRead : 0;
					KdPrint(("### Successfully read %u-%u=%u sectors (%lu bytes)\n", edx->FifoOut[5], pp->sector, bSectors, Irp->IoStatus.Information));
				}

				break;
			}

			case IOCTL_FDCMD_WRITE_DATA:
			case IOCTL_FDCMD_WRITE_DELETED_DATA:
			{
				KdPrint(("In IOCTL_FDCMD_WRITE_[DELETED_]DATA\n"));
				PFD_READ_WRITE_PARAMS pp = (PFD_READ_WRITE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				ULONG DataSize = 0;

				status = CheckBuffers(Irp, sizeof(FD_READ_WRITE_PARAMS), 1);

				if (NT_SUCCESS(status))
				{
					UCHAR bSectors = pp->eot - pp->sector;

					if (!bSectors)
						status = STATUS_INVALID_PARAMETER;
					else if (pp->size > 7)
					{
						DataSize = stack->Parameters.DeviceIoControl.OutputBufferLength;
						KdPrint(("### Using supplied data buffer size of %lu bytes\n", DataSize));
					}
					else
					{
						DataSize = bSectors * SectorSize(pp->size); // safe
						status = CheckBuffers(Irp, 0, DataSize);
						KdPrint(("### Expected data size = %lu bytes (status=%08lx)\n", DataSize, status));
					}
				}

				if (NT_SUCCESS(status) && edx->WaitSector)
					status = WaitSector(edx, pp->flags, pp->phead, edx->WaitSectorCount);

				if (NT_SUCCESS(status) && edx->ShortWrite)
				{
					// Cap to the maximum DMA transfer size
					if (DataSize > edx->MaxTransferSize)
						DataSize = edx->MaxTransferSize;

					if (edx->DmaThreshold > DataSize)
						edx->ShortWrite = FALSE;
					else
					{
						edx->DmaThreshold = DataSize - edx->DmaThreshold;

						KdPrint(("### Short write of %lu out of %lu, with finetune of %lu\n", edx->DmaThreshold, DataSize, edx->DmaDelay));
						status = StartDmaThread(edx, ShortCommandThreadProc);
					}
				}

				if (NT_SUCCESS(status))
				{
					if (edx->ShortWrite)
						KeWaitForSingleObject(&edx->dmasync, Executive, KernelMode, FALSE, NULL);

					UCHAR bCommand = (IoCode == IOCTL_FDCMD_WRITE_DATA) ? COMMND_WRITE_DATA : COMMND_WRITE_DELETED_DATA;
					status = CommandReadWrite(edx, Irp->MdlAddress, DataSize, bCommand, pp->flags, pp->phead, pp->cyl, pp->head, pp->sector, pp->size, pp->eot, pp->gap, pp->datalen);

					if (edx->ShortWrite)
					{
						StopDmaThread(edx);
						edx->ShortWrite = FALSE;

						// If the short write triggered, clear the expected error
						if (edx->NeedReset && status == STATUS_FLOPPY_ID_MARK_NOT_FOUND)
							status = STATUS_SUCCESS;
					}
				}

				if (NT_SUCCESS(status))
				{
					UCHAR bSectors = edx->FifoOut[5] - pp->sector;
					ULONG ulWritten = bSectors * SectorSize(pp->size); // safe
					Irp->IoStatus.Information = (ulWritten <= DataSize) ? ulWritten : 0;
					KdPrint(("### Successfully wrote %u-%u=%u sectors (%lu bytes)\n", edx->FifoOut[5], pp->sector, bSectors, Irp->IoStatus.Information));
				}

				break;
			}

			case IOCTL_FDCMD_VERIFY:
			{
				PFD_READ_WRITE_PARAMS pp = (PFD_READ_WRITE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_READ_WRITE_PARAMS), sizeof(FD_CMD_RESULT), true);

				if (NT_SUCCESS(status) && edx->WaitSector)
					status = WaitSector(edx, pp->flags, pp->phead, edx->WaitSectorCount);

				if (NT_SUCCESS(status))
					status = CommandVerify(edx, pp->flags, pp->phead, pp->cyl, pp->head, pp->sector, pp->size, pp->eot, pp->gap, pp->datalen);

				if (NT_SUCCESS(status))
					CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));

				break;
			}

			case IOCTL_FDCMD_LOCK:
			{
				PFD_LOCK_PARAMS pp = (PFD_LOCK_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_LOCK_PARAMS), sizeof(FD_LOCK_RESULT), true);

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandLock(edx, pp->lock)))
					CopyOutput(edx, Irp, sizeof(FD_LOCK_RESULT));

				break;
			}

			case IOCTL_FDCMD_SENSE_DRIVE_STATUS:
			{
				PFD_SENSE_PARAMS pp = (PFD_SENSE_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_SENSE_PARAMS), sizeof(FD_DRIVE_STATUS));

				if (NT_SUCCESS(status) && NT_SUCCESS(status = CommandSenseDriveStatus(edx, pp->head)))
					CopyOutput(edx, Irp, sizeof(FD_DRIVE_STATUS));

				break;
			}

			case IOCTL_FDCMD_FORMAT_TRACK:
			{
				PFD_FORMAT_PARAMS pp = (PFD_FORMAT_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				ULONG DataSize = sizeof(FD_ID_HEADER)*pp->sectors;

				status = CheckBuffers(Irp, sizeof(FD_FORMAT_PARAMS), sizeof(FD_CMD_RESULT), true);

				if (NT_SUCCESS(status))
					status = CheckBuffers(Irp, sizeof(FD_FORMAT_PARAMS) + DataSize, 0);

				if (NT_SUCCESS(status) && edx->ShortWrite)
				{
					if (edx->DmaThreshold > DataSize)
						edx->ShortWrite = FALSE;
					else
					{
						edx->DmaThreshold = DataSize - edx->DmaThreshold;

						if (NT_SUCCESS(status = StartDmaThread(edx, ShortCommandThreadProc)))
						{
							KdPrint(("### Short format of %lu out of %lu, with finetune of %lu\n", edx->DmaThreshold, DataSize, edx->DmaDelay));
							KeWaitForSingleObject(&edx->dmasync, Executive, KernelMode, FALSE, NULL);
						}
					}
				}

				if (NT_SUCCESS(status))
				{
					 if (NT_SUCCESS(status = FormatTrack(edx, Irp)))
						CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));

					if (edx->ShortWrite)
					{
						StopDmaThread(edx);
						edx->ShortWrite = FALSE;

						// If the short write triggered, clear the expected error
						if (edx->NeedReset && status == STATUS_FLOPPY_ID_MARK_NOT_FOUND)
							status = STATUS_SUCCESS;
					}
				}

				break;
			}

			case IOCTL_FDCMD_FORMAT_AND_WRITE:
			{
				PFD_FORMAT_PARAMS pp = (PFD_FORMAT_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_FORMAT_PARAMS), sizeof(FD_CMD_RESULT), true);

				PUCHAR pbStart = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
				PUCHAR pb = pbStart + sizeof(FD_FORMAT_PARAMS);

				for (ULONG u = 0 ; NT_SUCCESS(status) && u < pp->sectors ; u++)
				{
					PFD_ID_HEADER pid = (PFD_ID_HEADER)pb;
					pb += sizeof(FD_ID_HEADER);

					if (NT_SUCCESS(status = CheckBuffers(Irp, (ULONG)(pb-pbStart), 0)))
						pb += SectorSize(pid->size);
				}

				ULONG DataSize = (ULONG)(pb-pbStart);
				if (NT_SUCCESS(status))
					status = CheckBuffers(Irp, DataSize, 0);

				if (NT_SUCCESS(status) && edx->ShortWrite)
				{
					if (edx->DmaThreshold > DataSize)
						edx->ShortWrite = FALSE;
					else
					{
						edx->DmaThreshold = DataSize - edx->DmaThreshold;

						if (NT_SUCCESS(status = StartDmaThread(edx, ShortCommandThreadProc)))
						{
							KdPrint(("### Short format of %lu out of %lu, with finetune of %lu\n", edx->DmaThreshold, DataSize, edx->DmaDelay));
							KeWaitForSingleObject(&edx->dmasync, Executive, KernelMode, FALSE, NULL);
						}
					}
				}

				if (NT_SUCCESS(status))
				{
					 if (NT_SUCCESS(status = FormatAndWrite(edx, Irp, DataSize)))
						CopyOutput(edx, Irp, sizeof(FD_CMD_RESULT));

					if (edx->ShortWrite)
					{
						StopDmaThread(edx);
						edx->ShortWrite = FALSE;

						// If the short write triggered, clear the expected error
						if (edx->NeedReset && status == STATUS_FLOPPY_ID_MARK_NOT_FOUND)
							status = STATUS_SUCCESS;
					}
				}

				break;
			}

			case IOCTL_FD_SCAN_TRACK:
			{
				PFD_SCAN_PARAMS pp = (PFD_SCAN_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				ULONG InSize = stack->Parameters.DeviceIoControl.InputBufferLength;
				status = CheckBuffers(Irp, sizeof(FD_SCAN_PARAMS), sizeof(FD_SCAN_RESULT));

				// Support the old scan structure
				if (NT_SUCCESS(status) && InSize == 3)
				{
					PUCHAR pb = (PUCHAR)Irp->AssociatedIrp.SystemBuffer;
					KdPrint(("Old IOCTL_FD_SCAN_TRACK params: %02X %02X %02X\n", pb[0], pb[1], pb[2]));

					// Correct to new-style parameters
					UCHAR cyl = pb[1];
					pb[1] = pb[2];

					// If we need to recalibrate, do it now before we seek
					if (edx->NeedRecalibrate && NT_SUCCESS(status = CommandRecalibrate(edx)))
						edx->NeedRecalibrate = FALSE;

					// Seek to the track we're scanning
					if (NT_SUCCESS(status))
						status = CommandSeek(edx, pp->flags, cyl);
				}

				if (NT_SUCCESS(status))
					status = TimedScanTrack(edx, Irp);

				if (NT_SUCCESS(status))
				{
					PFD_TIMED_SCAN_RESULT pd = (PFD_TIMED_SCAN_RESULT)edx->IoBuffer;

					ULONG DataSize = sizeof(FD_SCAN_RESULT) + sizeof(FD_ID_HEADER)*pd->count;
					ULONG OutSize = IoGetCurrentIrpStackLocation(Irp)->Parameters.DeviceIoControl.OutputBufferLength;
					KdPrint(("OutSize=%lu, DataSize=%lu\n", OutSize, DataSize));

					if (DataSize > OutSize)
						status = STATUS_BUFFER_TOO_SMALL;
					else
					{
						PFD_SCAN_RESULT po = (PFD_SCAN_RESULT)Irp->AssociatedIrp.SystemBuffer;
						po->count = pd->count;

						for (UCHAR i = 0 ; i < pd->count ; i++)
						{
							po->Header[i].cyl = pd->Header[i].cyl;
							po->Header[i].head = pd->Header[i].head;
							po->Header[i].sector = pd->Header[i].sector;
							po->Header[i].size = pd->Header[i].size;
						}

						Irp->IoStatus.Information = DataSize;
					}
				}

				break;
			}

			case IOCTL_FD_TIMED_SCAN_TRACK:
			{
				status = CheckBuffers(Irp, sizeof(FD_SCAN_PARAMS), sizeof(FD_TIMED_SCAN_RESULT));

				if (NT_SUCCESS(status))
					status = TimedScanTrack(edx, Irp);

				if (NT_SUCCESS(status))
				{
					PFD_TIMED_SCAN_RESULT pd = (PFD_TIMED_SCAN_RESULT)edx->IoBuffer;
					ULONG DataSize = sizeof(FD_TIMED_SCAN_RESULT) + sizeof(FD_TIMED_ID_HEADER)*pd->count;
					ULONG OutSize = IoGetCurrentIrpStackLocation(Irp)->Parameters.DeviceIoControl.OutputBufferLength;
					KdPrint(("OutSize=%lu, DataSize=%lu\n", OutSize, DataSize));

					if (DataSize > OutSize)
						status = STATUS_BUFFER_TOO_SMALL;
					else
					{
						RtlCopyMemory(Irp->AssociatedIrp.SystemBuffer, edx->IoBuffer, DataSize);
						Irp->IoStatus.Information = DataSize;
					}
				}

				break;
			}

			case IOCTL_FD_RAW_READ_TRACK:
			{
				KdPrint(("In IOCTL_FD_RAW_READ_TRACK(2)\n"));
				PFD_RAW_READ_PARAMS pp = (PFD_RAW_READ_PARAMS)Irp->AssociatedIrp.SystemBuffer;
				status = CheckBuffers(Irp, sizeof(FD_RAW_READ_PARAMS), 128);
				ULONG DataSize = 0;

				// Ensure the data size requested is sensible, and the supplied buffer is large enough
				if (NT_SUCCESS(status))
				{
					if (pp->size > 7)
					{
						DataSize = stack->Parameters.DeviceIoControl.OutputBufferLength;
						KdPrint(("### Using supplied data buffer size of %lu bytes\n", DataSize));
					}
					else
					{
						DataSize = SectorSize(pp->size); // safe
						status = CheckBuffers(Irp, 0, DataSize);
						KdPrint(("### Expected data size = %lu bytes (status=%08lx)\n", DataSize, status));
					}
				}

				// Select the 2nd unit containing the PC-formatted disk
				if (NT_SUCCESS(status))
				{
					KdPrint(("Switching to Alt device\n"));
					edx->DeviceUnit ^= 1;
					edx->DriveOnValue ^= 1;
					RestoreUnit(edx);
				}

				// Read status register 3
				if (NT_SUCCESS(status))
				{
					status = CommandSenseDriveStatus(edx, pp->head);
					KdPrint(("Alt ST3 = %02X\n", edx->FifoOut[0]));
				}

				// If the disk isn't at track 0, recalibrate, saving the main physical head position around it
				if (NT_SUCCESS(status) && !(edx->FifoOut[0] & STREG3_TRACK_0))
				{
					KdPrint(("Not at track 0, recalibrating...\n"));
					ULONG PhysCyl = edx->PhysCyl;
					status = CommandRecalibrate(edx);
					edx->PhysCyl = PhysCyl;

				}

				// Set the switch parameters and start the thread
				if (NT_SUCCESS(status))
				{
					// Cap to the maximum DMA transfer size
					if (DataSize > edx->MaxTransferSize)
						DataSize = edx->MaxTransferSize;

					edx->DmaThreshold = DataSize-1;
					status = StartDmaThread(edx, DorSwitchThreadProc);
				}

				if (NT_SUCCESS(status))
				{
					// Wait until the thread signals it's waiting
					KeWaitForSingleObject(&edx->dmasync, Executive, KernelMode, FALSE, NULL);

					// Select high density rate
					SetFdcDataRate(edx, 0);

					// Start reading the PC sector, ready for the DOR switching thread to pounce
					status = CommandReadTrack(edx, Irp->MdlAddress, DataSize, pp->flags, pp->head, 0, 0, 1, pp->size, 1+1, 1, 0xff);

					// End the thread, which should have already finished
					StopDmaThread(edx);

					// Restore the data rate if it's not the 500Kbps we used above
					if (edx->DataRate != 0)
						SetFdcDataRate(edx, edx->DataRate);

					// A CRC error is expected for raw reads, so ignore it
					if (status == STATUS_CRC_ERROR)
						status = STATUS_SUCCESS;
				}

				// Restore the original drive selection, whatever happened, but keep both motors on
				KdPrint(("Restoring original device\n"));
				edx->DeviceUnit = edx->pdx->DeviceUnit;
				edx->DriveOnValue = (FDC_MOTOR_A|FDC_MOTOR_B) | (FDC_SELECT_A + edx->DeviceUnit);
				RestoreUnit(edx);

				// If successful, report the data size being returned
				if (NT_SUCCESS(status))
					Irp->IoStatus.Information = DataSize;

				break;
			}

			case IOCTL_FD_MOTOR_ON:
			{
				status = STATUS_SUCCESS;
				Irp->IoStatus.Information = 0;
				break;
			}

			case IOCTL_FD_WAIT_INDEX:
			{
				status = WaitIndex(edx);
				Irp->IoStatus.Information = 0;
				break;
			}

			case IOCTL_FD_CHECK_DISK:
			{
				status = STATUS_SUCCESS;
				Irp->IoStatus.Information = 0;
				break;
			}

			case IOCTL_FD_GET_TRACK_TIME:
			{
				status = CheckBuffers(Irp, 0, sizeof(UINT32));

				if (NT_SUCCESS(status))
					status = WaitIndex(edx, true);

				if (NT_SUCCESS(status))
					*(PUINT32)Irp->AssociatedIrp.SystemBuffer = edx->SpinTime;

				Irp->IoStatus.Information = sizeof(UINT32);
				break;
			}

			default:
				KdPrint(("Failing unknown IOCTL\n"));
				Irp->IoStatus.Information = 0;
				status = STATUS_UNSUCCESSFUL;
				break;
		}

		// Complete the DeviceIoControl code
		CompleteRequest(Irp, status, Irp->IoStatus.Information);
	}

	if (edx->Enabled)
		DisableFdc(edx);

	if (edx->Acquired)
	{
		KdPrint(("### Leaving ThreadProc (Acquired)\n"));
		ResetFdc(edx);
		ReleaseFdc(edx);
		edx->Acquired = FALSE;
	}

	FlFreeIoBuffer(edx);

	KdPrint((DRIVERNAME " - Leaving ThreadProc()\n"));
	PsTerminateSystemThread(STATUS_SUCCESS);
}
