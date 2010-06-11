#include "System/Buffers.h"

int main(void)
{
	StaticArray<>	s;
	
	s.Writef("hello world");
	
	printf("%.*s\n", s.Length(), s.Buffer());
	
	ByteWrap bw = s.WrapRest();
	bw.Writef("123");
	
	printf("%.*s\n", bw.Length(), bw.Buffer());
	
	s.Lengthen(bw.Length());
	printf("%.*s\n", s.Length(), s.Buffer());
	
	
//	DynArray<8>		buffer;
//	ByteString		str("hello hello");
//	ByteWrap		wrap;
//	HeapArray		heap;
		
//	buffer.Writef("%B", &str);
//	printf("%.*s\n", buffer.Length(), buffer.Buffer());
//	
//	str.Set("hello world");
//
//	buffer.Writef("%B %d", &str, 15);
//	printf("%.*s\n", buffer.Length(), buffer.Buffer());
//	
//	str = buffer.ToByteString();
//	printf("%.*s\n", str.Length(), str.Buffer());
//	
//	wrap = buffer;
//	printf("%.*s\n", wrap.Length(), wrap.Buffer());
//
//	wrap = buffer.WrapRest();
//	printf("%.*s\n", wrap.Length(), wrap.Buffer());
//	
//	buffer.Lengthen(buffer.WrapRest().Writef(" !"));
//	printf("%.*s\n", buffer.Length(), buffer.Buffer());	
//	
//	buffer.Appendf(" %d %d %d", 1, 2, 3);
//	printf("%.*s\n", buffer.Length(), buffer.Buffer());
	
//	buffer.Write("this is a new string");
//	printf("%.*s\n", buffer.Length(), buffer.Buffer());
}
