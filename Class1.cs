using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using UnityEngine;

namespace MyGlobalSignalTower
{
    [BepInPlugin("com.myself.globalsignaltower", "Global Signal Tower Ultimate", "1.5.1")]
    public class GlobalSignalTowerMod : BaseUnityPlugin
    {
        internal static BepInEx.Logging.ManualLogSource Log;

        public static ConfigEntry<float> Cfg_PowerConnect;
        public static ConfigEntry<float> Cfg_PowerCover;
        public static ConfigEntry<float> Cfg_GroundSignalRange;
        public static ConfigEntry<float> Cfg_SpaceSignalRange;
        public static ConfigEntry<float> Cfg_BattleBasePickRange;
        public static ConfigEntry<float> Cfg_BattleBaseConstructRange;
        public static ConfigEntry<float> Cfg_GaussTurretAttackRange;
        public static ConfigEntry<float> Cfg_PlasmaMaxSpeed;
        public static ConfigEntry<bool> Cfg_EnableMarkAll;
        public static ConfigEntry<bool> Cfg_EnablePlasmaTurretPatch;

        private static int lastStarId = -1;
        private static int update_frame_counter = 0;
        private const int REFRESH_INTERVAL = 60;
        private const int LIFE_TICK = 120;
        private static readonly object markLock = new object();

        public static Dictionary<int, GlobalEnemyHashSystem> planetHashSystems = new Dictionary<int, GlobalEnemyHashSystem>();

        void Awake()
        {
            Log = Logger;

            Cfg_PowerConnect = Config.Bind("1. 信号塔", "电力连接距离", 60.5f, new ConfigDescription("", new AcceptableValueRange<float>(60.5f, 630f)));
            Cfg_PowerCover = Config.Bind("1. 信号塔", "电力覆盖半径", 14.5f, new ConfigDescription("", new AcceptableValueRange<float>(14.5f, 630f)));
            Cfg_GroundSignalRange = Config.Bind("1. 信号塔", "地面信号范围", 700f, new ConfigDescription("", new AcceptableValueRange<float>(50f, 5000f)));
            Cfg_SpaceSignalRange = Config.Bind("1. 信号塔", "太空信号范围", 4200f, new ConfigDescription("", new AcceptableValueRange<float>(100f, 50000f)));
            Cfg_BattleBasePickRange = Config.Bind("2. 战场基站", "拾取范围", 90f, new ConfigDescription("", new AcceptableValueRange<float>(90f, 630f)));
            Cfg_BattleBaseConstructRange = Config.Bind("2. 战场基站", "建造范围", 60f, new ConfigDescription("", new AcceptableValueRange<float>(60f, 630f)));
            Cfg_GaussTurretAttackRange = Config.Bind("3. 炮塔增强", "通用攻击范围", 100000f, new ConfigDescription("", new AcceptableValueRange<float>(100f, 1000000f)));
            Cfg_PlasmaMaxSpeed = Config.Bind("3. 炮塔增强", "等离子弹速", 500000f, new ConfigDescription("", new AcceptableValueRange<float>(20000f, 1000000f)));
            Cfg_EnableMarkAll = Config.Bind("4. 开关", "启用全局标记", true, "");
            Cfg_EnablePlasmaTurretPatch = Config.Bind("4. 开关", "启用等离子增强", true, "");

            Cfg_PowerConnect.SettingChanged += (s, e) => ApplySettings();
            Cfg_PowerCover.SettingChanged += (s, e) => ApplySettings();
            Cfg_GroundSignalRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_BattleBasePickRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_BattleBaseConstructRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_GaussTurretAttackRange.SettingChanged += (s, e) => ApplySettings();

            new Harmony("com.myself.globalsignaltower").PatchAll();
            Log.LogInfo("【全局信号塔】1.5.1 加载完成。");
        }

        void Update()
        {
            if (GameMain.instance == null || GameMain.isPaused || !GameMain.isRunning) return;

            // 1. 管理哈希系统 (主线程安全)
            ManageHashSystems();

            // 2. 其他低频逻辑
            update_frame_counter++;
            if (update_frame_counter < REFRESH_INTERVAL) return;
            update_frame_counter = 0;

            RefreshBattleBaseRanges();
        }

        private void ManageHashSystems()
        {
            int currentStarId = GameMain.localStar?.id ?? -1;
            if (currentStarId != -1 && currentStarId != lastStarId)
            {
                lastStarId = currentStarId;
                planetHashSystems.Clear();
                if (GameMain.data?.factoryCount > 0)
                {
                    for (int i = 0; i < GameMain.data.factoryCount; i++)
                    {
                        PlanetFactory factory = GameMain.data.factories[i];
                        if (factory?.planet?.star?.id == currentStarId)
                        {
                            planetHashSystems[factory.planetId] = new GlobalEnemyHashSystem(factory);
                        }
                    }
                }
            }
        }

        // 【新增】在游戏开始时强制重置，防止第二次读档报错
        [HarmonyPatch(typeof(GameMain), "Begin")]
        class Patch_GameStart {
            [HarmonyPostfix]
            static void Postfix() {
                planetHashSystems.Clear();
                lastStarId = -1;
                Log.LogInfo("游戏开始，已重置哈希系统。");
            }
        }
        public static void MarkAllEnemies(PlanetFactory factory)
        {
            if (!Cfg_EnableMarkAll.Value || factory == null) return;

            // 检查该星球是否有信号塔
            if (!TryGetBeaconPosition(factory, out _)) return;

            if (!planetHashSystems.TryGetValue(factory.planetId, out var hashSystem)) return;

            DefenseSystem defenseSystem = factory.defenseSystem;
            if (defenseSystem == null) return;

            // 【关键】在多线程环境下调用 AddGlobalTargets 必须加锁
            lock (markLock)
            {
                hashSystem.MarkAllEnemiesSpliced(defenseSystem, LIFE_TICK);
            }
        }
        public static bool TryGetBeaconPosition(PlanetFactory factory, out Vector3 beaconPos)
        {
            beaconPos = Vector3.zero;
            if (factory.defenseSystem?.beacons?.buffer == null) return false;
            var pool = factory.defenseSystem.beacons.buffer;
            int cursor = factory.defenseSystem.beacons.cursor;
            for (int i = 1; i < cursor; i++)
            {
                if (pool[i].id == i && pool[i].entityId > 0)
                {
                    beaconPos = factory.entityPool[pool[i].entityId].pos;
                    return true;
                }
            }
            return false;
        }

        [HarmonyPatch(typeof(VFPreload), "InvokeOnLoadWorkEnded")]
        class Patch_Init { [HarmonyPostfix] public static void Postfix() => ApplySettings(); }

        public static void ApplySettings()
        {
            var signal = LDB.items.Select(3007);
            if (signal?.prefabDesc != null)
            {
                signal.prefabDesc.powerConnectDistance = Cfg_PowerConnect.Value;
                signal.prefabDesc.powerCoverRadius = Cfg_PowerCover.Value;
                signal.prefabDesc.beaconSignalRadius = 2000f;
            }
            var bab = LDB.items.Select(3009);
            if (bab?.prefabDesc != null)
            {
                bab.prefabDesc.battleBasePickRange = Cfg_BattleBasePickRange.Value;
                bab.prefabDesc.constructionRange = Cfg_BattleBaseConstructRange.Value;
            }
            var plasma = LDB.items.Select(3004);
            if (plasma?.prefabDesc != null)
            {
                plasma.prefabDesc.turretSpaceAttackRange = Cfg_GaussTurretAttackRange.Value;
                plasma.prefabDesc.turretMaxAttackRange = Cfg_GaussTurretAttackRange.Value;
                plasma.prefabDesc.turretPitchUpMax = 90f;
                plasma.prefabDesc.turretPitchDownMax = 20f;
            }
            var gauss = LDB.items.Select(3001);
            if (gauss?.prefabDesc != null)
            {
                gauss.prefabDesc.turretMaxAttackRange = Cfg_GaussTurretAttackRange.Value;
            }
        }

        private void RefreshBattleBaseRanges()
        {
            var factory = GameMain.localPlanet?.factory;
            if (factory?.defenseSystem?.battleBases?.buffer == null) return;
            var pool = factory.defenseSystem.battleBases.buffer;
            int cursor = factory.defenseSystem.battleBases.cursor;
            for (int i = 1; i < cursor; i++)
            {
                if (pool[i].id == i)
                {
                    pool[i].pickRange = Cfg_BattleBasePickRange.Value;
                    pool[i].constructRange = Cfg_BattleBaseConstructRange.Value;
                }
            }
        }

        [HarmonyPatch(typeof(DFSDynamicHashSystem), "GameTick")]
        public static class Patch_UpdateGlobalHash
        {
            [HarmonyPostfix]
            public static void Postfix(DFSDynamicHashSystem __instance)
            {
                if (__instance.factory != null)
                {
                    // 1. 更新哈希表数据
                    if (planetHashSystems.TryGetValue(__instance.factory.planetId, out var sys))
                    {
                        sys.GameTick();
                        
                        // 2. 【彦祖要求】在这里执行标记逻辑
                        // 这样每个星球每帧只跑一次，且支持全宇宙星球
                        MarkAllEnemies(__instance.factory);
                    }
                }
            }
        }
        [HarmonyPatch(typeof(TurretComponent), "Shoot_Plasma")]
        public static class Patch_ExtendBulletLife
        {
            static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
                CodeMatcher matcher = new CodeMatcher(instructions);
                var lifeField = AccessTools.Field(typeof(GeneralProjectile), "life");
                var lifeMaxField = AccessTools.Field(typeof(GeneralProjectile), "lifemax");
                int newLife = 12000;
                matcher.MatchForward(false, new CodeMatch(OpCodes.Stfld, lifeField));
                if (matcher.IsValid)
                {
                    matcher.Advance(-1);
                    if (matcher.Opcode == OpCodes.Ldc_I4_S || matcher.Opcode == OpCodes.Ldc_I4)
                        matcher.SetAndAdvance(OpCodes.Ldc_I4, newLife);
                }
                matcher.MatchForward(false, new CodeMatch(OpCodes.Stfld, lifeMaxField));
                if (matcher.IsValid)
                {
                    matcher.Advance(-1);
                    if (matcher.Opcode == OpCodes.Ldc_I4_S || matcher.Opcode == OpCodes.Ldc_I4)
                        matcher.SetAndAdvance(OpCodes.Ldc_I4, newLife);
                }
                return matcher.InstructionEnumeration();
            }
        }

        [HarmonyPatch(typeof(TurretComponent), "Shoot_Plasma")]
        public static class Patch_IncreasePlasmaSpeed
        {
            static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
                CodeMatcher matcher = new CodeMatcher(instructions);
                while (matcher.MatchForward(false, new CodeMatch(OpCodes.Ldc_R8, 20000.0)).IsValid)
                {
                    matcher.SetOperandAndAdvance((double)Cfg_PlasmaMaxSpeed.Value);
                }
                return matcher.InstructionEnumeration();
            }
        }

        [HarmonyPatch(typeof(TurretComponent), "Search_Plasma")]
        public static class Patch_PlasmaSearch
        {
            [HarmonyPostfix]
            public static void Postfix(ref TurretComponent __instance, PlanetFactory factory, PrefabDesc pdesc)
            {
                if (!Cfg_EnablePlasmaTurretPatch.Value) return;
                if (__instance.target.id > 0) return;
                if (!planetHashSystems.TryGetValue(factory.planetId, out var hashSystem)) return;

                var caps = __instance.vsCaps;
                var settings = __instance.vsSettings;
                int checkOrbit = ((int)(caps & settings & VSLayerMask.OrbitHigh)) >> 4;
                int checkSpace = ((int)(caps & settings & VSLayerMask.SpaceHigh)) >> 6;
                if (checkOrbit == 0 && checkSpace == 0) return;

                Vector3 turretPos = factory.entityPool[__instance.entityId].pos;
                
                double num5 = Math.Cos((double)(90f - pdesc.turretPitchDownMax) * 0.01745329238474369);
                double num6 = Math.Cos((double)(90f + pdesc.turretPitchUpMax) * 0.01745329238474369);

                IDPOS_Ex targetData = hashSystem.FindNearestSpaceEnemyFast(
                    turretPos, pdesc.turretSpaceAttackRange, num5, num6, checkOrbit > 0, checkSpace > 0
                );

                if (targetData.id > 0)
                {
                    __instance.target.id = targetData.id;
                    __instance.target.astroId = targetData.originAstroId;
                    __instance.target.type = ETargetType.Enemy;
                    __instance.isLockingTarget = true;
                }
            }
        }
    }

    public struct IDPOS_Ex
    {
        public int id;
        public int originAstroId;
        public Vector3 pos;
        public bool isSpaceUnit;

        public IDPOS_Ex(int id, int originAstroId, Vector3 pos, bool isSpace)
        {
            this.id = id;
            this.originAstroId = originAstroId;
            this.pos = pos;
            this.isSpaceUnit = isSpace;
        }
    }

    public class GlobalEnemyHashSystem
    {
        private PlanetFactory factory;
        private SpaceSector sector;
        private PlanetData planetData;

        private const int LEVEL_COUNT = 9;
        public static readonly double[] LEVEL_OFFSETS_SQR = new double[]
        {
            0.0, 4000000.0, 9000000.0, 25000000.0, 49000000.0,
            196000000.0, 784000000.0, 3136000000.0, 10000000000.0
        };
        private const double MAX_DIST_SQR = 10000000000.0;

        private IDPOS_Ex[] hashDatas;
        private IDPOS_Ex[] tmp_datas;
        private int[] dataCursors;
        private int[] tmp_cursors;
        private HashSet<int> old_ids_space;
        private HashSet<int> old_ids_ground;

        private int currentMarkLevel = 0;
        private int currentMarkIndex = 0;
        private int capacityPerLevel = 4096;

        private int groundScanCursor = 1;
        private int spaceScanCursor = 1;

        public GlobalEnemyHashSystem(PlanetFactory factory)
        {
            this.factory = factory;
            this.sector = factory.sector;
            this.planetData = factory.planet;
            int total = capacityPerLevel * LEVEL_COUNT;
            hashDatas = new IDPOS_Ex[total];
            tmp_datas = new IDPOS_Ex[total];
            dataCursors = new int[LEVEL_COUNT];
            tmp_cursors = new int[LEVEL_COUNT];
            old_ids_space = new HashSet<int>();
            old_ids_ground = new HashSet<int>();
        }

        public void GameTick()
        {
            // 【安全修复】检查 factory 是否还活着
            if (factory == null || factory.enemyPool == null || sector == null || sector.enemyPool == null) return;

            var swapData = tmp_datas; tmp_datas = hashDatas; hashDatas = swapData;
            var swapCursor = tmp_cursors; tmp_cursors = dataCursors; dataCursors = swapCursor;
            Array.Clear(hashDatas, 0, hashDatas.Length);
            Array.Clear(dataCursors, 0, dataCursors.Length);
            old_ids_space.Clear();
            old_ids_ground.Clear();

            VectorLF3 uPos = planetData.uPosition;
            int currentAstroId = planetData.astroId;

            for (int lvl = 0; lvl < LEVEL_COUNT; lvl++)
            {
                int start = lvl * capacityPerLevel;
                int end = start + tmp_cursors[lvl];
                for (int i = start; i < end; i++)
                {
                    IDPOS_Ex oldData = tmp_datas[i];
                    if (oldData.id == 0) continue;
                    if (TryAddEnemy(oldData.id, oldData.originAstroId, uPos, currentAstroId, oldData.isSpaceUnit))
                    {
                        if (oldData.isSpaceUnit) old_ids_space.Add(oldData.id);
                        else old_ids_ground.Add(oldData.id);
                    }
                }
            }
            ScanNewGroundEnemies(uPos, currentAstroId);
            ScanNewSpaceEnemies(uPos, currentAstroId);
        }

        private bool TryAddEnemy(int id, int originAstroId, VectorLF3 uPos, int currentAstroId, bool isSpace)
        {
            // 【安全修复】再次检查引用
            if (factory == null || sector == null) return false;
            EnemyData[] pool = isSpace ? sector.enemyPool : factory.enemyPool;
            if (pool == null || id < 0 || id >= pool.Length) return false;
            ref EnemyData enemy = ref pool[id];
            if (enemy.id != id || enemy.isInvincible) return false;
            if (enemy.combatStatId > 0 && factory.skillSystem.combatStats.buffer[enemy.combatStatId].hp <= 0) return false;

            VectorLF3 wPos;
            sector.TransformFromAstro_ref(enemy.astroId, out wPos, ref enemy.pos);
            double sqrDist = (wPos - uPos).sqrMagnitude;
            if (sqrDist > MAX_DIST_SQR) return false;

            int level = 0;
            for (int i = LEVEL_COUNT - 1; i >= 0; i--)
            {
                if (sqrDist >= LEVEL_OFFSETS_SQR[i]) { level = i; break; }
            }

            if (dataCursors[level] < capacityPerLevel)
            {
                VectorLF3 lPosLF;
                sector.InverseTransformToAstro_ref(currentAstroId, ref wPos, out lPosLF);
                int idx = level * capacityPerLevel + dataCursors[level];
                hashDatas[idx] = new IDPOS_Ex(id, originAstroId, (Vector3)lPosLF, isSpace);
                dataCursors[level]++;
                return true;
            }
            return false;
        }

        private void ScanNewGroundEnemies(VectorLF3 uPos, int currentAstroId)
        {
            if (factory == null || factory.enemyPool == null) return;
            var pool = factory.enemyPool;
            int maxCursor = factory.enemyCursor;
            if (maxCursor <= 1) return;
            for (int i = 0; i < 500; i++)
            {
                groundScanCursor++;
                if (groundScanCursor >= maxCursor) groundScanCursor = 1;
                if (old_ids_ground.Contains(groundScanCursor)) continue;
                ref EnemyData e = ref pool[groundScanCursor];
                if (e.id == groundScanCursor && e.id != 0)
                    TryAddEnemy(e.id, e.originAstroId, uPos, currentAstroId, false);
            }
        }

        private void ScanNewSpaceEnemies(VectorLF3 uPos, int currentAstroId)
        {
            if (sector == null || sector.enemyPool == null) return;
            var pool = sector.enemyPool;
            int maxCursor = sector.enemyCursor;
            if (maxCursor <= 1) return;
            for (int i = 0; i < 500; i++)
            {
                spaceScanCursor++;
                if (spaceScanCursor >= maxCursor) spaceScanCursor = 1;
                if (old_ids_space.Contains(spaceScanCursor)) continue;
                ref EnemyData e = ref pool[spaceScanCursor];
                if (e.id == spaceScanCursor && e.id != 0)
                    TryAddEnemy(e.id, e.originAstroId, uPos, currentAstroId, true);
            }
        }

        public void MarkAllEnemiesSpliced(DefenseSystem ds, int lifeTick)
        {
            int totalActive = 0;
            for (int l = 0; l <= 3; l++) totalActive += dataCursors[l];
            if (totalActive == 0) return;
            int batchSize = (totalActive / 60) + 1;
            for (int i = 0; i < batchSize; i++)
            {
                int safety = 0;
                while (currentMarkIndex >= dataCursors[currentMarkLevel] && safety < 4)
                {
                    currentMarkIndex = 0;
                    currentMarkLevel++;
                    if (currentMarkLevel > 3) currentMarkLevel = 0;
                    safety++;
                }
                int arrayIdx = currentMarkLevel * capacityPerLevel + currentMarkIndex;
                IDPOS_Ex d = hashDatas[arrayIdx];
                if (d.id != 0)
                {
                    bool valid = d.isSpaceUnit ? (currentMarkLevel <= 3) : (currentMarkLevel == 0);
                    if (valid)
                    {
                        TimedSkillTarget target = default;
                        target.id = d.id;
                        target.astroId = d.originAstroId;
                        target.type = ETargetType.Enemy;
                        target.lifeTick = lifeTick;
                        ds.AddGlobalTargets(ref target);
                    }
                }
                currentMarkIndex++;
            }
        }

        public IDPOS_Ex FindNearestSpaceEnemyFast(Vector3 muzzlePos, float maxRange, double pDown, double pUp, bool relay, bool space)
        {
            float maxRangeSqr = maxRange * maxRange;
            var spacePool = sector.enemyPool;
            var stats = factory.skillSystem.combatStats.buffer;
            for (int lvl = 0; lvl < LEVEL_COUNT; lvl++)
            {
                int start = lvl * capacityPerLevel;
                int end = start + dataCursors[lvl];
                for (int i = start; i < end; i++)
                {
                    IDPOS_Ex d = hashDatas[i];
                    if (d.id == 0 || !d.isSpaceUnit) continue;
                    ref EnemyData e = ref spacePool[d.id];
                    if (e.id != d.id || e.isInvincible) continue;
                    bool isRelay = e.dfRelayId > 0;
                    if (isRelay && !relay) continue;
                    if (!isRelay && !space) continue;
                    if (e.combatStatId > 0 && stats[e.combatStatId].hp <= 0) continue;
                    if ((d.pos - muzzlePos).sqrMagnitude > maxRangeSqr) continue;
                    if (InternalCheckPitch(d.pos, muzzlePos, pDown, pUp)) return d;
                }
            }
            return default;
        }

        private bool InternalCheckPitch(Vector3 enemyPos, Vector3 muzzlePos, double pDown, double pUp)
        {
            float n1 = -muzzlePos.x; float n2 = -muzzlePos.y; float n3 = -muzzlePos.z;
            float n4 = enemyPos.x - muzzlePos.x; float n5 = enemyPos.y - muzzlePos.y; float n6 = enemyPos.z - muzzlePos.z;
            float n7 = (float)Math.Sqrt((double)(n1 * n1 + n2 * n2 + n3 * n3));
            float n8 = (float)Math.Sqrt((double)(n4 * n4 + n5 * n5 + n6 * n6));
            if (n7 < 1E-06f || n8 < 1E-06f) return false;
            double cosValue = (double)((n1 * n4 + n2 * n5 + n3 * n6) / (n7 * n8));
            if (cosValue < -1.0) cosValue = -1.0; else if (cosValue > 1.0) cosValue = 1.0;
            return cosValue <= pDown - 1E-06 && cosValue >= pUp + 1E-06;
        }

        public IDPOS_Ex[] GetHashDatas() => hashDatas;
        public int[] GetDataCursors() => dataCursors;
        public int GetCapacityPerLevel() => capacityPerLevel;
        public int GetTotalCount() { int sum = 0; for (int i = 0; i < LEVEL_COUNT; i++) sum += dataCursors[i]; return sum; }
    }
}