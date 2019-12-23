package ch.descabato.hashes;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by josh on 16/04/2014.
 * Not thread-safe, not nothin not nohow :)
 */
public class BuzHash {

    // via sublime text column mode, and cat /dev/random | hexdump
    static private final int[] randomInts = {
            0x185e036c, 0x75d8b628, 0xb9e9449c, 0x304a0c3f,
            0x671f124c, 0xb12837df, 0x6d8bfaf0, 0xc0aa233c,
            0xe51a03e9, 0xac6f2736, 0x5dc83037, 0x5ae4caca,
            0x626cd285, 0xe91ddc0e, 0xf8777856, 0xfb2ed5ab,
            0x2b4a9f3c, 0x9ec25937, 0x3933732c, 0xd2f01530,
            0x49d04005, 0xcf5d42ec, 0x2d83c806, 0x50072b11,
            0xc1084a43, 0xcefc72eb, 0xd767b732, 0x5f3dc856,
            0x5532ff5c, 0xbc5fe3f1, 0x71e4d5f0, 0xf2b01bd2,
            0x58c240c1, 0x8f46d29d, 0xf7665aa8, 0xb2c3301c,
            0x98a6229a, 0x901c660d, 0x593a690f, 0x674fca34,
            0x09d02e77, 0x8bce3391, 0xd37a3ca5, 0x85723a04,
            0x6e18e371, 0xf5b15e63, 0x8e70b29f, 0xb2282ee6,
            0x99071a45, 0x35e6ec46, 0x095d142f, 0xd0b53f71,
            0x8c664c23, 0x6c7e86c4, 0xcdcff641, 0xa7570929,
            0xebd4a528, 0xdc4c5a48, 0x08f5439a, 0x71fe13d5,
            0xe6bff286, 0x781a7742, 0xe93554f7, 0xe11346e7,
            0x4b2fde77, 0x420b6843, 0x27126a66, 0x1f65af88,
            0xc3476091, 0x834afc3a, 0xf04fa28f, 0x65b518d8,
            0x45ca961c, 0xe6485684, 0x09daaded, 0xe95365f4,
            0x78ae1f41, 0x2b7b3178, 0xfad99bc5, 0x4495bb73,
            0x02b51fd8, 0xf3340c63, 0x5b3babeb, 0xb68f9c2f,
            0x1d5a48a8, 0x1a2c6776, 0x9a8188ce, 0x43a99d09,
            0xa00058ab, 0x25931c94, 0xd76e3f21, 0x14dcff2d,
            0x0134fc42, 0x654bbec6, 0x55a913c0, 0x5444f0b7,
            0x9cb62ad6, 0xfdd05be9, 0x0b709a0d, 0x29f58c9a,
            0xa7c44094, 0xc64f60ad, 0x06461ef2, 0xbfdb9f65,
            0x572e8445, 0xdfecfdcc, 0xaafb077f, 0x70ac7a2e,
            0xe9a49683, 0x5239cc55, 0xcd0ccef2, 0x3b7d0c16,
            0x41af9948, 0x356bdd4e, 0x24f65f4e, 0xec678c5c,
            0x20016c14, 0x9a26eaf4, 0x69bd364a, 0xe8548717,
            0xac45c772, 0xfccbdd40, 0x79e730f0, 0x90074389,
            0x0c5498af, 0x7d351c64, 0x94df091c, 0x6a7aeecf,
            0x7d1b935f, 0x0904ea02, 0x98ce872e, 0xf98a6f2f,
            0x4e1b6699, 0x6cee1ad0, 0x9c0d4fb1, 0xe1393e36,
            0x68a76719, 0x35f64949, 0xc2e602d8, 0xb8429d1b,
            0xcd75f30c, 0xebebfd19, 0x97a32839, 0x232acf7f,
            0xb61a0f01, 0xe80bddc4, 0x18c58414, 0x8b3ab488,
            0x1405d391, 0xdb21cf52, 0x0a89abd1, 0x1e79664c,
            0x4ea52d60, 0x820940bc, 0x6ae925e2, 0x0c9fba94,
            0x12bcf2b7, 0xeffacf70, 0x4c477924, 0x6ade73ec,
            0xdf2bb8b9, 0xe1477f56, 0xa5213425, 0x1f35633f,
            0x2dbb6308, 0x0dbd8b23, 0x7b4f6daf, 0xad893ad0,
            0x966ad389, 0x73f6c914, 0x6377536c, 0x83a1fcc3,
            0xc1909513, 0x3ad6eadf, 0xb985693a, 0x63327d1f,
            0x123aaeaf, 0xb55a6e97, 0xb41f4bf8, 0xc04f6b18,
            0x270aeb63, 0x65fe8e7a, 0xa28517d1, 0xc6725623,
            0xe070eb5b, 0x33c7c8ed, 0xab3e1943, 0xbc2d3809,
            0x98894cb5, 0xc4c71bea, 0x5ea78269, 0x2c2a3066,
            0xde55089b, 0x56cd1a60, 0x81c4758a, 0xf68e30aa,
            0xad4676a2, 0xc619cf5f, 0x2cca8431, 0x6d7b8bb2,
            0x46f72136, 0x7805f34a, 0x6b3f0f23, 0x085dc9c9,
            0x9bc1f7a2, 0xc9053b4d, 0x78718505, 0xb6256572,
            0x2ea045a6, 0x53fa6b5c, 0x742b5180, 0x0838a379,
            0x73033da8, 0x74df2555, 0xbd0d9921, 0x76a16d47,
            0x20be57f7, 0xa192dd25, 0xdc213106, 0xfaa2fbbf,
            0x6fa0ce03, 0xc0a8df66, 0xabbf4245, 0x2b1a0ab2,
            0xc4027cc6, 0x4dfb03db, 0xfc668cdd, 0xa942e590,
            0xdea21be3, 0xf33ef053, 0x8e00c366, 0x2884e400,
            0x47c92ea2, 0x1a1ebc00, 0xe27060f5, 0x15ff085d,
            0xa10c4693, 0x0aefb3af, 0xd166044d, 0x7a1af93c,
            0x9d4a049e, 0x820fa698, 0x83042297, 0xc624911f,
            0xeea26983, 0xe34676b0, 0xcd89bc0f, 0x4ae669f2,
            0x4427adec, 0x0cb84a61, 0x27d3cd6d, 0x17787be1,
            0x0c61920f, 0xfaec8d30, 0xced092dc, 0xf16734b7
    };

    private byte[] buffer;
    private int bufferIndex;
    private int bufferSize;
    private int sizeMod32;
    private int state;

    public BuzHash(int size) {

        super();

        initialize(size);
    }

    private void initialize(int size) {
        bufferIndex = 0;
        state = 0;
        bufferSize = size;
        sizeMod32 = size % 32;
        buffer = new byte[size];

        // Initialise buffer to a fixed state based on size (so we don't have to bother with overflow tracking)
        byte seedByte = (byte) (size & 0xff);

        for (int i = 0; i < size; i++) {
            state = Integer.rotateLeft(state, 1);
            state ^= randomInts[seedByte & 0xff]; // & 0xff to keeps it unsigned when converting to int for index
            buffer[i] = seedByte;
            seedByte++;
        }
    }

    public List<Integer> updateAndReportBoundaries(byte[] bs, int len, byte boundaryBits) {
        long bitmask = createBitmask(boundaryBits);
        LinkedList<Integer> retVal = new LinkedList<>();
        for (int i = 0; i < len; i++) {
            byte b = bs[i];
            addByte(b);
            if ((bitmask & state) == 0 && state != 0) {
                retVal.add(i);
                reset();
            }
        }
        return retVal;
    }

    public int updateAndReportBoundary(byte[] bs, int offset, int len, byte boundaryBits) {
        long bitmask = createBitmask(boundaryBits);

        int end = len + offset;
        for (int i = offset; i < end; i++) {
            byte b = bs[i];
            addByte(b);
            if ((bitmask & state) == 0 && state != 0) {
                return i - offset;
            }
        }
        return -1;
    }

    private long createBitmask(int boundaryBits) {
        long lo = 1;
        for (int i = 0; i < boundaryBits; i++) {
            lo = lo * 2 + 1;
        }
        return lo;
    }

    public int getHash() {
        return state;
    }

    public int addByte(byte b) {

        byte oldByte = buffer[bufferIndex];

        // Progress state
        state = Integer.rotateLeft(state, 1);

        // Remove oldByte
        state ^= Integer.rotateLeft(randomInts[oldByte & 0xff], sizeMod32);

        // Add newByte
        state ^= randomInts[b & 0xff];
        buffer[bufferIndex] = b;

        bufferIndex++;
        bufferIndex %= bufferSize;

        return state;
    }

    public void reset() {
        initialize(bufferSize);
    }

}