// stddcls.h -- Precompiled headers for WDM drivers

#ifdef __cplusplus
extern "C" {
#endif

#pragma warning(disable:4201)	// nameless struct/union
#pragma warning(disable:4200) // zero-sized array in struct/union
#pragma warning(disable:4815) // zero-sized array in stack object will have no elements
#pragma warning(disable:4706) // assignment within conditional expression (old code!)

#define DEPRECATE_DDK_FUNCTIONS 1

#include <ntddk.h>
#include <stdio.h>

#ifdef __cplusplus
}
#endif

#define PAGEDCODE code_seg("PAGE")
#define LOCKEDCODE code_seg()
#define INITCODE code_seg("INIT")

#define PAGEDDATA data_seg("PAGEDATA")
#define LOCKEDDATA data_seg()
#define INITDATA data_seg("INITDATA")

#define arraysize(p) (sizeof(p)/sizeof((p)[0]))

#if DBG
	#undef ASSERT
	#define ASSERT(e) if(!(e)){DbgPrint("Assertion failure in "\
	__FILE__ ", line %d: " #e "\n", __LINE__);\
	__debugbreak();\
  }
#endif

#if !DBG
#pragma warning(disable:4390)	// empty controlled statement
#endif

typedef unsigned char	BYTE;
typedef unsigned short	WORD;
typedef unsigned long	DWORD;
