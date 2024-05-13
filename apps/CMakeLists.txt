add_library(apps.base INTERFACE)
target_link_libraries(apps.base INTERFACE nats.base)

# ----- nats -----
add_executable(apps.nats)
target_sources(apps.nats PUBLIC Main.cpp)
# add_sanitizers(nats)
# set_target_properties(nats PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
target_link_libraries(apps.nats PUBLIC apps.base)

