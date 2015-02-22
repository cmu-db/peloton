//TODO include tile header file

#pragma once

namespace nstore {

class AbstractPlanNode {
  public:
    bool init();
    Tile getNextTile();
    void cleanup();
  protected:
    virtual bool p_init();
    virtual Tile p_getNextTile();
    virtual void p_cleanup();
};

} // namespace nstore
