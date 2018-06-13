#pragma once

namespace peloton{
namespace brain{
enum RunMode{ ActualRun = 0, DryRun = 1 };
enum CandidateSelectionType{ Simple = 0, AutoAdmin = 1, Exhaustive = 2};
}
}