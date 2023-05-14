namespace CsvRx.Core;

public enum ApplyOrder
{
    None,
    TopDown,
    BottomUp
}

public enum VisitRecursion
{
    Continue,
    Skip,
    Stop
}

public enum StatisticType
{
    Population,
    Sample
}

public enum JoinType
{
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
    /// Left Semi Join
    LeftSemi,
    /// Right Semi Join
    RightSemi,
    /// Left Anti Join
    LeftAnti,
    /// Right Anti Join
    RightAnti,
}

public enum JoinSide
{
    /// Left side of the join
    Left,
    /// Right side of the join
    Right,
}

public enum PartitionMode
{
    /// Left/right children are partitioned using the left and right keys
    Partitioned,
    /// Left side will collected into one partition
    CollectLeft,
    /// When set to Auto, DataFusion optimizer will decide which PartitionMode mode(Partitioned/CollectLeft) is optimal based on statistics.
    /// It will also consider swapping the left and right inputs for the Join
    Auto,
}